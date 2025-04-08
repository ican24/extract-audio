fmt:
    cargo fmt

release: fmt
    cargo build --release

archive:
    ouch compress dist/extract-audio_aarch64-apple-darwin dist/extract-audio_aarch64-apple-darwin.zip
    ouch compress dist/extract-audio_aarch64-unknown-linux-gnu dist/extract-audio_aarch64-unknown-linux-gnu.zip
    ouch compress dist/extract-audio_x86_64-apple-darwin dist/extract-audio_x86_64-apple-darwin.zip
    ouch compress dist/extract-audio_x86_64-unknown-linux-gnu dist/extract-audio_x86_64-unknown-linux-gnu.zip
    ouch compress dist/extract-audio_x86_64-unknown-linux-musl dist/extract-audio_x86_64-unknown-linux-musl.zip
