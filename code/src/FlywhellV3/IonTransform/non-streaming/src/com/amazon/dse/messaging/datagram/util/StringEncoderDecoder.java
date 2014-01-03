package com.amazon.dse.messaging.datagram.util;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

/**
 * @author kaitchuc
 * A simple wrapper class to do string transcoding.
 */
public class StringEncoderDecoder {

	static final Charset _default_encoding = Charset.forName("UTF-8");
	static final CharsetDecoder _default_decoder = _default_encoding.newDecoder();
	static final CharsetEncoder _default_encoder = _default_encoding.newEncoder();
	private final boolean _is_utf8;
	final CharsetDecoder decoder;
	final CharsetEncoder encoder;

	/**
	 * Create a utf-8 string encoder/decoder.
	 */
	public StringEncoderDecoder() {
		_is_utf8 = true;
		decoder = StringEncoderDecoder._default_decoder;
		encoder = StringEncoderDecoder._default_encoder;
	}
	
	/**
	 * Create a new string encoder / decoder using the provided encoding.
	 * @param charEncoding
	 */
	public StringEncoderDecoder(String charEncoding) {
		Charset charset;
		if (charEncoding!=null) {
			charset = Charset.forName(charEncoding);
			_is_utf8 = StringEncoderDecoder._default_encoding.equals(charset);
		} else { 
			charset = StringEncoderDecoder._default_encoding;
			_is_utf8 = true;
		}
		if (_is_utf8) {
			encoder = StringEncoderDecoder._default_encoder;
			decoder = StringEncoderDecoder._default_decoder;
		} else {
			decoder = charset.newDecoder();
			encoder = charset.newEncoder();
		}
	}
	
	/**
	 * Reads a string out of a chunk.
	 * 
	 * @return a new string composed from the next <code>number</code> bytes.
	 */
	public String readString(Chunk c, int length) {
		String result;
		if (c.pos_+length > c.length_)
			throw new IllegalStateException("You asked to read more data than exists.");
		CharBuffer buf = CharBuffer.allocate(length);
		byte ch;
		if (_is_utf8) {
			for (int i = 0; i < length; i++) {
				ch = c.data_[c.start_ + c.pos_ + i];

				if ((ch & 0x80) == 0)
					buf.put((char) ch);
				else if ((ch & 0xe0) == 0xc0) {
					byte ch1 = c.data_[c.start_ + c.pos_ + i + 1];
					buf.put((char) (((ch & 0x1f) << 6) + (ch1 & 0x3f)));
				} else {
					if (decoder.decode(ByteBuffer.wrap(c.data_, c.start_+c.pos_+i, length-i),buf,true).isMalformed())
						throw new IllegalStateException("Invalid UTF8 string.");
					break;
				}
			}
		} else {
			if (decoder.decode(ByteBuffer.wrap(c.data_, c.start_+c.pos_, length),buf,true).isMalformed())
				throw new IllegalStateException("Invalid string.");
		}
		buf.limit(buf.position()).rewind();
		result = buf.toString();
		c.pos_ += length;
		return result;
	}
	
	/**
	 * @param s The string you want to encode.
	 * @return A byte buffer representing the contents of the provided string.
	 */
	public ByteBuffer encodeString(String s) {
		int length = s.length();
		int maxLength = (int)(encoder.maxBytesPerChar()*s.length())+1;
		ByteBuffer bb = ByteBuffer.allocate(maxLength);
		if (_is_utf8) {
			char ch;
			for (int i = 0; i < length; i++) {
				ch = s.charAt(i);

				if (ch < 0x80)
					bb.put((byte)ch);
				else if (ch < 0x800) {
					bb.put((byte)(0xc0 + ((ch >> 6) & 0x1f)));
					bb.put((byte)(0x80 + (ch & 0x3f)));
				} else {
					if(encoder.encode(CharBuffer.wrap(s,i,length),bb,true).isMalformed())
						throw new IllegalArgumentException("Invalid UTF8 string.");
					break;
				}
			}
		} else {
			if(encoder.encode(CharBuffer.wrap(s,0,length),bb,true).isMalformed())
				throw new IllegalArgumentException("Invalid string.");
		}
		bb.limit(bb.position()).rewind();
		return bb;
	}
	
}
