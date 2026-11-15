# Install

## Download a binary

The quickest way. Grab the archive for your system from the
[releases page](https://github.com/SpollaL/datasight/releases), unpack it, and put
`datasight` somewhere on your `PATH`:

| System | File |
|---|---|
| Linux (Intel/AMD) | `datasight-x86_64-unknown-linux-gnu.tar.xz` |
| Linux (ARM) | `datasight-aarch64-unknown-linux-gnu.tar.xz` |
| macOS (Apple silicon) | `datasight-aarch64-apple-darwin.tar.xz` |
| macOS (Intel) | `datasight-x86_64-apple-darwin.tar.xz` |
| Windows | `datasight-x86_64-pc-windows-msvc.zip` |

> [!NOTE]
> These downloads can't browse Azure or S3. For cloud storage, build it yourself as
> shown below.

## Build it with cargo

You need Rust 1.75 or newer ([rustup](https://rustup.rs) installs it).

```bash
cargo install datasight
```

## With cloud storage

Cloud support is optional, so it's switched on when you build:

```bash
cargo install datasight --features azure        # Azure Blob Storage
cargo install datasight --features aws          # Amazon S3
cargo install datasight --features azure,aws    # both
```

How to log in to each is covered in [Files and the cloud](browse.md#cloud-storage).

## Check it works

```bash
datasight --version
```
