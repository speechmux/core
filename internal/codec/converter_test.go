package codec

import (
	"encoding/binary"
	"math"
	"testing"

	clientpb "github.com/speechmux/proto/gen/go/client/v1"
)

// makePCM builds a PCM S16LE byte slice from int16 samples.
func makePCM(samples []int16) []byte {
	buf := make([]byte, len(samples)*2)
	for i, s := range samples {
		binary.LittleEndian.PutUint16(buf[i*2:], uint16(s))
	}
	return buf
}

// readPCM reads all int16 samples from a PCM S16LE byte slice.
func readPCM(buf []byte) []int16 {
	out := make([]int16, len(buf)/2)
	for i := range out {
		out[i] = int16(binary.LittleEndian.Uint16(buf[i*2:]))
	}
	return out
}

// ── Passthrough ──────────────────────────────────────────────────────────────

func TestConvert_PCM_S16LE_Passthrough(t *testing.T) {
	c := NewCodecConverter(16000)
	data := makePCM([]int16{100, 200, -100, -200})

	out, err := c.Convert(data, clientpb.AudioEncoding_AUDIO_ENCODING_PCM_S16LE, 16000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if &out[0] != &data[0] {
		// Same underlying array means zero copy.
		// (We accept re-allocated equal slices too.)
	}
	if len(out) != len(data) {
		t.Fatalf("length mismatch: got %d want %d", len(out), len(data))
	}
	for i := range out {
		if out[i] != data[i] {
			t.Errorf("byte %d: got %d want %d", i, out[i], data[i])
		}
	}
}

func TestConvert_Unspecified_Passthrough(t *testing.T) {
	c := NewCodecConverter(16000)
	data := makePCM([]int16{1, 2, 3})

	out, err := c.Convert(data, clientpb.AudioEncoding_AUDIO_ENCODING_UNSPECIFIED, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != len(data) {
		t.Fatalf("length mismatch")
	}
}

// ── A-law ────────────────────────────────────────────────────────────────────

func TestDecodeAlaw_Silence(t *testing.T) {
	// A-law 0xD5 encodes silence (0) in standard G.711.
	sample := decodeAlaw(0xD5)
	if sample < -16 || sample > 16 {
		t.Errorf("A-law silence: expected ~0, got %d", sample)
	}
}

func TestDecodeAlaw_AllBytes_NoPanic(t *testing.T) {
	for i := 0; i < 256; i++ {
		_ = decodeAlaw(byte(i))
	}
}

func TestConvert_Alaw_DefaultRate(t *testing.T) {
	c := NewCodecConverter(16000)
	// Single A-law sample; result should be resampled 8000 → 16000.
	alaw := []byte{0xD5} // near-zero sample
	out, err := c.Convert(alaw, clientpb.AudioEncoding_AUDIO_ENCODING_ALAW, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 1 input sample @ 8kHz → 2 output samples @ 16kHz (2× upsampling).
	if len(out) != 4 {
		t.Errorf("expected 4 bytes (2 samples × 2 bytes), got %d", len(out))
	}
}

// ── mu-law ───────────────────────────────────────────────────────────────────

func TestDecodeMulaw_Silence(t *testing.T) {
	// mu-law 0x7F and 0xFF encode silence-level values.
	s := decodeMulaw(0x7F)
	if s < -100 || s > 100 {
		t.Errorf("mu-law near-silence: expected ~0, got %d", s)
	}
}

func TestDecodeMulaw_AllBytes_NoPanic(t *testing.T) {
	for i := 0; i < 256; i++ {
		_ = decodeMulaw(byte(i))
	}
}

func TestConvert_Mulaw_Explicit8k(t *testing.T) {
	c := NewCodecConverter(16000)
	mulaw := []byte{0x7F}
	out, err := c.Convert(mulaw, clientpb.AudioEncoding_AUDIO_ENCODING_MULAW, 8000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 1 input sample @ 8kHz → 2 output samples @ 16kHz.
	if len(out) != 4 {
		t.Errorf("expected 4 bytes, got %d", len(out))
	}
}

// ── Resampling ───────────────────────────────────────────────────────────────

func TestResample_8kTo16k_LengthDoubles(t *testing.T) {
	samples := make([]int16, 100)
	for i := range samples {
		samples[i] = int16(i * 100)
	}
	pcm := makePCM(samples)

	out, err := resampleS16LE(pcm, 8000, 16000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := int(math.Round(float64(len(samples)) * 2.0))
	if len(out)/2 != want {
		t.Errorf("expected %d output samples, got %d", want, len(out)/2)
	}
}

func TestResample_SameRate_IsNoop(t *testing.T) {
	data := makePCM([]int16{1, 2, 3})
	out, err := resampleS16LE(data, 16000, 16000)
	if err != nil {
		t.Fatal(err)
	}
	if &out[0] != &data[0] {
		// Allow copy; just check length and values.
	}
	if len(out) != len(data) {
		t.Errorf("same-rate resample changed length: %d → %d", len(data), len(out))
	}
}

func TestResample_OddLength_Error(t *testing.T) {
	_, err := resampleS16LE([]byte{1, 2, 3}, 8000, 16000)
	if err == nil {
		t.Fatal("expected error for odd-length input")
	}
}

func TestResample_16kTo8k_LengthHalves(t *testing.T) {
	samples := make([]int16, 160)
	pcm := makePCM(samples)
	out, err := resampleS16LE(pcm, 16000, 8000)
	if err != nil {
		t.Fatal(err)
	}
	if len(out)/2 != 80 {
		t.Errorf("expected 80 samples, got %d", len(out)/2)
	}
}

// ── WAV extraction ────────────────────────────────────────────────────────────

// buildWAV constructs a minimal RIFF/WAVE header around the given PCM data.
func buildWAV(pcm []byte, sampleRate int) []byte {
	dataSize := len(pcm)
	fmtSize := 16
	chunkSize := 4 + 8 + fmtSize + 8 + dataSize

	buf := make([]byte, 12+8+fmtSize+8+dataSize)
	i := 0
	put := func(b []byte) { copy(buf[i:], b); i += len(b) }
	putU16 := func(v uint16) { binary.LittleEndian.PutUint16(buf[i:], v); i += 2 }
	putU32 := func(v uint32) { binary.LittleEndian.PutUint32(buf[i:], v); i += 4 }

	put([]byte("RIFF"))
	putU32(uint32(chunkSize))
	put([]byte("WAVE"))

	put([]byte("fmt "))
	putU32(uint32(fmtSize))
	putU16(1)                    // PCM
	putU16(1)                    // mono
	putU32(uint32(sampleRate))   // sample rate
	putU32(uint32(sampleRate * 2)) // byte rate
	putU16(2)                    // block align
	putU16(16)                   // bits per sample

	put([]byte("data"))
	putU32(uint32(dataSize))
	put(pcm)
	return buf
}

func TestWavExtract_RoundTrip(t *testing.T) {
	samples := []int16{100, 200, -100, -200, 0}
	pcm := makePCM(samples)
	wav := buildWAV(pcm, 16000)

	extracted, sr, err := wavExtractPCM(wav)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sr != 16000 {
		t.Errorf("sample rate: got %d want 16000", sr)
	}
	got := readPCM(extracted)
	for i, s := range samples {
		if got[i] != s {
			t.Errorf("sample[%d]: got %d want %d", i, got[i], s)
		}
	}
}

func TestConvert_WAV_WithResample(t *testing.T) {
	samples := make([]int16, 80) // 80 samples @ 8kHz = 10ms
	pcm := makePCM(samples)
	wav := buildWAV(pcm, 8000)

	c := NewCodecConverter(16000)
	out, err := c.Convert(wav, clientpb.AudioEncoding_AUDIO_ENCODING_WAV, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 80 samples @ 8kHz → 160 samples @ 16kHz
	if len(out)/2 != 160 {
		t.Errorf("expected 160 output samples, got %d", len(out)/2)
	}
}

// ── Error cases ───────────────────────────────────────────────────────────────

func TestConvert_OGGOpus_Unsupported(t *testing.T) {
	c := NewCodecConverter(16000)
	_, err := c.Convert([]byte{0x4f, 0x67, 0x67, 0x53}, clientpb.AudioEncoding_AUDIO_ENCODING_OGG_OPUS, 0)
	if err == nil {
		t.Fatal("expected error for OGG_OPUS")
	}
}

func TestConvert_UnknownEncoding_Error(t *testing.T) {
	c := NewCodecConverter(16000)
	_, err := c.Convert([]byte{1, 2}, clientpb.AudioEncoding(99), 0)
	if err == nil {
		t.Fatal("expected error for unknown encoding")
	}
}

func TestConvert_InvalidWAV_Error(t *testing.T) {
	c := NewCodecConverter(16000)
	_, err := c.Convert([]byte("not a WAV file"), clientpb.AudioEncoding_AUDIO_ENCODING_WAV, 0)
	if err == nil {
		t.Fatal("expected error for invalid WAV data")
	}
}
