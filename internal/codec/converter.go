// Package codec converts client audio encodings to PCM S16LE 16 kHz for the pipeline.
package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"runtime"

	sttErrors "github.com/speechmux/core/internal/errors"
	clientpb "github.com/speechmux/proto/gen/go/client/v1"
)

// resamplerSem bounds the number of concurrent CPU-intensive conversions.
// Excess requests are rejected with ErrUnsupportedEncoding rather than queuing
// behind CPU-bound work (design doc §10.14).
var resamplerSem = make(chan struct{}, runtime.NumCPU())

// alawTable and mulawTable are precomputed G.711 decode lookup tables.
var (
	alawTable  [256]int16
	mulawTable [256]int16
)

func init() {
	for i := 0; i < 256; i++ {
		alawTable[i] = decodeAlaw(byte(i))
		mulawTable[i] = decodeMulaw(byte(i))
	}
}

// CodecConverter converts audio from various encodings to PCM S16LE at a fixed
// target sample rate. All heavy conversions are protected by a per-process
// semaphore (design doc §10.14).
type CodecConverter struct {
	targetSampleRate int
}

// NewCodecConverter creates a CodecConverter targeting the given sample rate.
// If targetSampleRate is 0, 16000 is used.
func NewCodecConverter(targetSampleRate int) *CodecConverter {
	if targetSampleRate == 0 {
		targetSampleRate = 16000
	}
	return &CodecConverter{targetSampleRate: targetSampleRate}
}

// Convert converts data from encoding/sourceSampleRate to PCM S16LE at the
// target sample rate.
//
//   - PCM S16LE with matching sample rate is a zero-copy passthrough.
//   - All other conversions acquire the resampling semaphore; ErrUnsupportedEncoding
//     (ERR1015) is returned immediately when no slot is available.
//   - ErrCodecConversionFailed (ERR3003) is returned on conversion errors.
func (c *CodecConverter) Convert(
	data []byte,
	encoding clientpb.AudioEncoding,
	sourceSampleRate int32,
) ([]byte, error) {
	// Passthrough: PCM S16LE at the correct rate — no work needed.
	isPCM := encoding == clientpb.AudioEncoding_AUDIO_ENCODING_UNSPECIFIED ||
		encoding == clientpb.AudioEncoding_AUDIO_ENCODING_PCM_S16LE
	rateMatch := sourceSampleRate == 0 || int(sourceSampleRate) == c.targetSampleRate
	if isPCM && rateMatch {
		return data, nil
	}

	// All other paths are CPU-bound; acquire semaphore (non-blocking).
	select {
	case resamplerSem <- struct{}{}:
		defer func() { <-resamplerSem }()
	default:
		return nil, sttErrors.New(sttErrors.ErrUnsupportedEncoding,
			"codec conversion capacity exceeded: resampler semaphore full")
	}

	return c.convert(data, encoding, sourceSampleRate)
}

func (c *CodecConverter) convert(
	data []byte,
	encoding clientpb.AudioEncoding,
	sourceSampleRate int32,
) ([]byte, error) {
	switch encoding {
	case clientpb.AudioEncoding_AUDIO_ENCODING_UNSPECIFIED,
		clientpb.AudioEncoding_AUDIO_ENCODING_PCM_S16LE:
		// Resampling-only path (sample rate mismatch).
		src := int(sourceSampleRate)
		if src == 0 {
			return data, nil // assume already at target rate
		}
		out, err := resampleS16LE(data, src, c.targetSampleRate)
		if err != nil {
			return nil, sttErrors.New(sttErrors.ErrCodecConversionFailed, err.Error())
		}
		return out, nil

	case clientpb.AudioEncoding_AUDIO_ENCODING_ALAW:
		pcm := alawToPCM(data)
		src := int(sourceSampleRate)
		if src == 0 {
			src = 8000 // G.711 A-law standard rate
		}
		if src == c.targetSampleRate {
			return pcm, nil
		}
		out, err := resampleS16LE(pcm, src, c.targetSampleRate)
		if err != nil {
			return nil, sttErrors.New(sttErrors.ErrCodecConversionFailed, err.Error())
		}
		return out, nil

	case clientpb.AudioEncoding_AUDIO_ENCODING_MULAW:
		pcm := mulawToPCM(data)
		src := int(sourceSampleRate)
		if src == 0 {
			src = 8000 // G.711 mu-law standard rate
		}
		if src == c.targetSampleRate {
			return pcm, nil
		}
		out, err := resampleS16LE(pcm, src, c.targetSampleRate)
		if err != nil {
			return nil, sttErrors.New(sttErrors.ErrCodecConversionFailed, err.Error())
		}
		return out, nil

	case clientpb.AudioEncoding_AUDIO_ENCODING_WAV:
		pcm, sr, err := wavExtractPCM(data)
		if err != nil {
			return nil, sttErrors.New(sttErrors.ErrCodecConversionFailed, err.Error())
		}
		if sr == c.targetSampleRate {
			return pcm, nil
		}
		out, err := resampleS16LE(pcm, sr, c.targetSampleRate)
		if err != nil {
			return nil, sttErrors.New(sttErrors.ErrCodecConversionFailed, err.Error())
		}
		return out, nil

	case clientpb.AudioEncoding_AUDIO_ENCODING_OGG_OPUS:
		// OGG Opus decoding requires a native library (libopus).
		// This is not available in the pure-Go build.
		return nil, sttErrors.New(sttErrors.ErrUnsupportedEncoding,
			"OGG Opus decoding is not supported in this build")

	default:
		return nil, sttErrors.New(sttErrors.ErrUnsupportedEncoding,
			fmt.Sprintf("unknown audio encoding: %v", encoding))
	}
}

// ── Format converters ─────────────────────────────────────────────────────────

// alawToPCM decodes G.711 A-law bytes to PCM S16LE using the precomputed table.
func alawToPCM(data []byte) []byte {
	out := make([]byte, len(data)*2)
	for i, b := range data {
		binary.LittleEndian.PutUint16(out[i*2:], uint16(alawTable[b]))
	}
	return out
}

// mulawToPCM decodes G.711 mu-law bytes to PCM S16LE using the precomputed table.
func mulawToPCM(data []byte) []byte {
	out := make([]byte, len(data)*2)
	for i, b := range data {
		binary.LittleEndian.PutUint16(out[i*2:], uint16(mulawTable[b]))
	}
	return out
}

// decodeAlaw converts a single G.711 A-law byte to a 16-bit linear sample.
// Implements ITU-T G.711 §3.2.
func decodeAlaw(a byte) int16 {
	a ^= 0x55 // restore bit inversion applied at the encoder
	sign := (a & 0x80) != 0
	exp := (a >> 4) & 0x07
	mantissa := int32(a & 0x0F)

	var linear int32
	if exp == 0 {
		linear = (mantissa << 1) | 1
	} else {
		linear = ((mantissa | 0x10) << exp) | (1 << (exp - 1))
	}
	linear <<= 3 // scale to 16-bit range

	if !sign {
		return int16(-linear)
	}
	return int16(linear)
}

// decodeMulaw converts a single G.711 mu-law byte to a 16-bit linear sample.
// Implements ITU-T G.711 §3.1.
func decodeMulaw(u byte) int16 {
	const bias = 33
	u = ^u // flip all bits (complement coding)
	sign := (u & 0x80) != 0
	exp := (u >> 4) & 0x07
	mantissa := int32(u & 0x0F)

	linear := ((mantissa|0x10)<<1 + 1) << (exp + 2)
	linear -= bias

	if !sign {
		return int16(-linear)
	}
	return int16(linear)
}

// ── Resampler ─────────────────────────────────────────────────────────────────

// resampleS16LE resamples mono PCM S16LE data from srcRate to dstRate using
// linear interpolation. Returns an error if len(data) is odd.
func resampleS16LE(data []byte, srcRate, dstRate int) ([]byte, error) {
	if srcRate == dstRate {
		return data, nil
	}
	if len(data)%2 != 0 {
		return nil, fmt.Errorf("PCM data length must be even: %d bytes", len(data))
	}
	srcSamples := len(data) / 2
	if srcSamples == 0 {
		return data, nil
	}

	dstSamples := int(math.Round(float64(srcSamples) * float64(dstRate) / float64(srcRate)))
	if dstSamples == 0 {
		return []byte{}, nil
	}

	out := make([]byte, dstSamples*2)
	ratio := float64(srcRate) / float64(dstRate)

	for i := 0; i < dstSamples; i++ {
		srcPos := float64(i) * ratio
		srcIdx := int(srcPos)
		frac := srcPos - float64(srcIdx)

		s0 := int32(int16(binary.LittleEndian.Uint16(data[srcIdx*2:])))
		var s1 int32
		if srcIdx+1 < srcSamples {
			s1 = int32(int16(binary.LittleEndian.Uint16(data[(srcIdx+1)*2:])))
		} else {
			s1 = s0
		}

		sample := int16(float64(s0)*(1-frac) + float64(s1)*frac)
		binary.LittleEndian.PutUint16(out[i*2:], uint16(sample))
	}
	return out, nil
}

// ── WAV container parser ──────────────────────────────────────────────────────

// wavExtractPCM parses a RIFF/WAVE container in memory and returns the raw
// PCM S16LE data and sample rate.
//
// Only PCM S16LE mono (format tag 1, channels 1, bits 16) is accepted.
func wavExtractPCM(data []byte) (pcm []byte, sampleRate int, err error) {
	r := bytes.NewReader(data)

	var riffID [4]byte
	if _, err := io.ReadFull(r, riffID[:]); err != nil {
		return nil, 0, fmt.Errorf("read RIFF id: %w", err)
	}
	if string(riffID[:]) != "RIFF" {
		return nil, 0, fmt.Errorf("not a RIFF file")
	}
	if _, err := r.Seek(4, io.SeekCurrent); err != nil { // skip RIFF size
		return nil, 0, fmt.Errorf("seek past RIFF size: %w", err)
	}
	var waveID [4]byte
	if _, err := io.ReadFull(r, waveID[:]); err != nil {
		return nil, 0, fmt.Errorf("read WAVE id: %w", err)
	}
	if string(waveID[:]) != "WAVE" {
		return nil, 0, fmt.Errorf("not a WAVE file")
	}

	var fmtFound bool
	for {
		var chunkID [4]byte
		if _, err := io.ReadFull(r, chunkID[:]); err == io.EOF {
			break
		} else if err != nil {
			return nil, 0, fmt.Errorf("read chunk id: %w", err)
		}
		var chunkSize uint32
		if err := binary.Read(r, binary.LittleEndian, &chunkSize); err != nil {
			return nil, 0, fmt.Errorf("read chunk size: %w", err)
		}

		switch string(chunkID[:]) {
		case "fmt ":
			if chunkSize < 16 {
				return nil, 0, fmt.Errorf("fmt chunk too small: %d bytes", chunkSize)
			}
			fmtBuf := make([]byte, chunkSize)
			if _, err := io.ReadFull(r, fmtBuf); err != nil {
				return nil, 0, fmt.Errorf("read fmt chunk: %w", err)
			}
			if tag := binary.LittleEndian.Uint16(fmtBuf[0:2]); tag != 1 {
				return nil, 0, fmt.Errorf("unsupported WAV format tag %d (only PCM=1 supported)", tag)
			}
			channels := binary.LittleEndian.Uint16(fmtBuf[2:4])
			sampleRate = int(binary.LittleEndian.Uint32(fmtBuf[4:8]))
			bitsPerSample := binary.LittleEndian.Uint16(fmtBuf[14:16])
			if channels != 1 || bitsPerSample != 16 {
				return nil, 0, fmt.Errorf(
					"unsupported WAV format: channels=%d bits=%d (need mono 16-bit)",
					channels, bitsPerSample)
			}
			fmtFound = true

		case "data":
			if !fmtFound {
				return nil, 0, fmt.Errorf("data chunk before fmt chunk")
			}
			pcm = make([]byte, chunkSize)
			if _, err := io.ReadFull(r, pcm); err != nil {
				return nil, 0, fmt.Errorf("read data chunk: %w", err)
			}
			return pcm, sampleRate, nil

		default:
			if _, err := r.Seek(int64(chunkSize), io.SeekCurrent); err != nil {
				return nil, 0, fmt.Errorf("skip chunk %s: %w", chunkID, err)
			}
		}
	}
	return nil, 0, fmt.Errorf("data chunk not found in WAV file")
}
