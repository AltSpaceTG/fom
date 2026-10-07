package io.fom.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link FileChannel} that delegates to a real one but can be told to fail:
 * a positional write writes half its bytes and then throws (a disk filling up
 * mid-frame), and {@code truncate} can throw too.
 */
final class FailingFileChannel extends FileChannel {

    private final FileChannel delegate;
    private final AtomicBoolean failWrite;
    private final AtomicBoolean failTruncate;
    private final java.util.concurrent.atomic.AtomicInteger interruptWrites;
    private final java.util.concurrent.atomic.AtomicInteger interruptForces;

    FailingFileChannel(FileChannel delegate, AtomicBoolean failWrite, AtomicBoolean failTruncate) {
        this(delegate, failWrite, failTruncate, new java.util.concurrent.atomic.AtomicInteger());
    }

    /** {@code interruptWrites}: that many writes behave like an interrupt landing mid-write. */
    FailingFileChannel(FileChannel delegate, AtomicBoolean failWrite, AtomicBoolean failTruncate,
                       java.util.concurrent.atomic.AtomicInteger interruptWrites) {
        this(delegate, failWrite, failTruncate, interruptWrites, new java.util.concurrent.atomic.AtomicInteger());
    }

    /** {@code interruptForces}: that many {@code force} calls behave like an interrupt after a complete write. */
    FailingFileChannel(FileChannel delegate, AtomicBoolean failWrite, AtomicBoolean failTruncate,
                       java.util.concurrent.atomic.AtomicInteger interruptWrites,
                       java.util.concurrent.atomic.AtomicInteger interruptForces) {
        this.delegate = delegate;
        this.failWrite = failWrite;
        this.failTruncate = failTruncate;
        this.interruptWrites = interruptWrites;
        this.interruptForces = interruptForces;
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        if (interruptWrites.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            ByteBuffer half = src.slice();
            half.limit(src.remaining() / 2);
            delegate.write(half, position);
            close(); // what the JDK does to an interruptible channel
            throw new java.nio.channels.ClosedByInterruptException();
        }
        if (failWrite.get()) {
            ByteBuffer half = src.slice();
            half.limit(src.remaining() / 2);
            delegate.write(half, position);
            throw new IOException("No space left on device (injected)");
        }
        return delegate.write(src, position);
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
        if (failTruncate.get()) {
            throw new IOException("truncate failed (injected)");
        }
        delegate.truncate(size);
        return this;
    }

    @Override public int read(ByteBuffer dst) throws IOException { return delegate.read(dst); }
    @Override public long read(ByteBuffer[] dsts, int offset, int length) throws IOException { return delegate.read(dsts, offset, length); }
    @Override public int write(ByteBuffer src) throws IOException { return delegate.write(src); }
    @Override public long write(ByteBuffer[] srcs, int offset, int length) throws IOException { return delegate.write(srcs, offset, length); }
    @Override public long position() throws IOException { return delegate.position(); }
    @Override public FileChannel position(long newPosition) throws IOException { delegate.position(newPosition); return this; }
    /** Set to make that many {@code size()} calls behave like an interrupt (the call before a write). */
    final java.util.concurrent.atomic.AtomicInteger interruptSizes = new java.util.concurrent.atomic.AtomicInteger();

    @Override
    public long size() throws IOException {
        if (interruptSizes.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            close(); // what the JDK does to an interruptible channel
            throw new java.nio.channels.ClosedByInterruptException();
        }
        return delegate.size();
    }
    @Override
    public void force(boolean metaData) throws IOException {
        if (interruptForces.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            close(); // what the JDK does to an interruptible channel
            throw new java.nio.channels.ClosedByInterruptException();
        }
        delegate.force(metaData);
    }
    /** Set to make {@code transferTo} copy one byte and then fail as on a full disk. */
    final AtomicBoolean failTransferTo = new AtomicBoolean();

    /** Set to make {@code transferTo} throw this exception (e.g. an access-denied error). */
    final java.util.concurrent.atomic.AtomicReference<IOException> transferToError =
            new java.util.concurrent.atomic.AtomicReference<>();

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        IOException error = transferToError.get();
        if (error != null) throw error;
        if (failTransferTo.get()) {
            delegate.transferTo(position, Math.min(1, count), target);
            throw new IOException("No space left on device (injected)");
        }
        return delegate.transferTo(position, count, target);
    }
    @Override public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException { return delegate.transferFrom(src, position, count); }
    @Override public int read(ByteBuffer dst, long position) throws IOException { return delegate.read(dst, position); }
    @Override public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException { return delegate.map(mode, position, size); }
    @Override public FileLock lock(long position, long size, boolean shared) throws IOException { return delegate.lock(position, size, shared); }
    @Override public FileLock tryLock(long position, long size, boolean shared) throws IOException { return delegate.tryLock(position, size, shared); }
    @Override protected void implCloseChannel() throws IOException { delegate.close(); }
}
