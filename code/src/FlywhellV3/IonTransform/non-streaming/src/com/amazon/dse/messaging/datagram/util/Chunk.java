package com.amazon.dse.messaging.datagram.util;

import java.io.ByteArrayOutputStream;
import java.security.InvalidParameterException;


/**
 * A thin VERY UNSAFE wrapper arround a byte array for convience.
 * 
 * @author kaitchuc
 */
public class Chunk implements SerializedDatagram {

	byte[] data_;

	int start_;

	int length_;

	int pos_; // Number of bytes after start to begin reading from when
				// called.

	/**
	 * Create a new chunk backed by the provided byte array.
	 * (No copy is made)
	 */
	public Chunk(byte[] data) {
		this(data, 0, data.length);
	}

	/**
	 * Create a new chunk backed by the provided byte array.
	 * @param data The byte array you want to wrap.
	 * @param start The offset to start reading from.
	 * @param length How far to read before stopping.
	 */
	public Chunk(byte[] data, int start, int length) {
		reset(data, start, length);
	}

	private void reset(byte[] data, int start, int length) {
		data_ = data;
		start_ = start;
		length_ = length;
		pos_ = 0;
	}

	/**
	 * Write this whole chunk to the provided stream.
	 * 
	 * @see com.amazon.dse.messaging.datagram.util.SerializedDatagram#writeTo(java.io.ByteArrayOutputStream)
	 */
	public void writeTo(ByteArrayOutputStream bout) {
		bout.write(data_, start_, length_);
	}

	/**
	 * Returns the total length of this chunk.
	 * 
	 * @see com.amazon.dse.messaging.datagram.util.SerializedDatagram#getLength()
	 */
	public int getLength() {
		return length_;
	}

	/**
	 * Backup one byte Could go negitive but if so, calling code is broken
	 * anyway.
	 */
	public void backup() {
		pos_--;
	}

	/**
	 * Jump some point. Could go negitive but if so, calling code is broken
	 * anyway.
	 */
	public void jumpTo(int pos) {
		pos_ = pos;
	}

	/**
	 * Future reads will start from the begining of the chunk.
	 */
	public void reset() {
		pos_ = 0;
	}

	/**
	 * Could go past end but if so calling code is broken anyway.
	 * 
	 * @return the next byte.
	 */
	public byte read() {
		return data_[start_ + pos_++];
	}

	/**
	 * Reads a long string of bytes. Could go past end but if so calling code is
	 * broken anyway.
	 * 
	 * @parm length how man bytes you want.
	 * @return An array of the bytes retreived from the input stream.
	 */
	public byte[] readBytes(int length) {
		byte[] result = new byte[length];
		System.arraycopy(data_, start_ + pos_, result, 0, length);
		pos_ += length;
		return result;
	}

	/**
	 * Reads a long string of bytes as a chunk. (No copying takes place) Could
	 * go past end but if so calling code is broken anyway.
	 * 
	 * @parm length how man bytes you want.
	 * @return An chunk representing the bytes retreived from the input stream.
	 */
	public Chunk readChunk(int length) {
		Chunk result = new Chunk(data_, start_ + pos_, length);
		pos_ += length;
		return result;
	}

	/**
	 * Could go past end but if so calling code is broken anyway.
	 */
	public void readInto(ByteArrayOutputStream bout, int number) {
		bout.write(data_, start_ + pos_, number);
		pos_ += number;
	}
	
	/**
	 * @return true iff length_ <= 0
	 */
	public boolean isEmpty() {
		if (length_ <= 0)
			return true;
		else
			return false;
	}

	/**
	 * @return True iff there are more bytes that are readable.
	 */
	public boolean hasMore() {
		if (!isEmpty() && pos_ < length_ && pos_ >= 0)
			return true;
		else
			return false;
	}

	/**
	 * @return The current offset from start.
	 */
	public int getPos() {
		return pos_;
	}

	/**
	 * Removes a segment from the center of this chunk. The segment removed will
	 * be [pos1,pos2). This chunk will become the remaining data to the left of
	 * the segment removed, And a new chunk representing the remaining data to
	 * the right of the segment removed.
	 */
	public Chunk devideOutSegment(int pos1, int pos2) {
		if (pos1 < 0 || pos2 <= 0 || pos2 <= pos1 || pos1 >= length_
				|| pos2 > length_)
			throw new InvalidParameterException(
					"Cannot remove a segment that ends before it starts, or has negitive indexes.");
		Chunk result;
		if (length_ - pos2 > 0)
			result = new Chunk(data_, start_ + pos2, length_ - pos2);
		else
			result = null;
		reset(data_, start_, pos1);
		return result;
	}
}
