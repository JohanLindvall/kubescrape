package logchain

// Attribute keys marking a record whose body was cut at a size cap. Every
// producer that truncates stamps AttrTruncated; AttrOriginalLength (the
// pre-cut body length in bytes) is stamped only where the producer still
// holds the whole message when it cuts — journald reads the oversized
// message and measures it before clip.Runes. The tailer deliberately
// stamps AttrTruncated ALONE: its multiline caps discard the overflow inside
// the library, whose Entry reports only a Truncated bool (never the dropped
// size), and the entry's file-offset span is a different quantity (raw
// on-disk bytes including CRI framing and newlines), so the original body
// length is genuinely unknowable at its truncation site.
const (
	AttrTruncated      = "log.truncated"
	AttrOriginalLength = "log.original_length"
)
