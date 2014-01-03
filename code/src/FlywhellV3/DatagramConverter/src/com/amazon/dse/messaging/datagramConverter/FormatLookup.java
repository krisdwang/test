package com.amazon.dse.messaging.datagramConverter;


import java.util.*;

import org.apache.log4j.Logger;

import com.amazon.dse.messaging.datagramConverter.DatagramConverter;
import com.amazon.dse.messaging.datagramConverter.DecodingException;

public class FormatLookup {
    
    private static Logger logger = Logger.getLogger(FormatLookup.class);
        
    private final DatagramConverter DEFAULT_INSTANCE;
    
    private final Map/*<Integer, DatagramConverter>*/ decoders_ = new TreeMap/*<Integer, DatagramConverter>*/();

    private final Map/*<String, DatagramConverter>*/ encoders_ = new TreeMap/*<String, DatagramConverter>*/();
    
    {
        try {
            addConverter("com.amazon.dse.messaging.datagram.IonDatagramConverter");
        } catch (Exception e) {
            logger.info("com.amazon.dse.messaging.datagram.IonDatagramConverter is not available.");
        }
        
        try {
            addConverter("amazon.platform.jms.HessianEncoderDecoder");
        } catch (Exception e) {
            logger.info("amazon.platform.jms.HessianEncoderDecoder is not available.");
        }
        
        try {
            addConverter("amazon.platform.clienttoolkit.util.TibrvdDatagramConverter");
        } catch (Exception e) {
            logger.info("amazon.platform.clienttoolkit.util.TibcoEncoderDecoder is not available");
        }
        
        DEFAULT_INSTANCE = (DatagramConverter) encoders_.get("Tibco");
    }

    /**
     * Add a new DatagramConverter to the list of encoders/decodres that this
     * class will support.
     * 
     * Note that it is not nessicary to call this function to use ether Tibco or
     * AmazonHessian formats as these are added by default. Also note that
     * because this class is designed to allow you to be selective in which
     * formats you want in your enviroment, this you will not get an exception
     * if it cannot instanciate the class you specify until you actually try to
     * use that decoder.
     * 
     * @param bytes
     *            The magic bytes that will identify a message of the format
     *            that the converter supports.
     * @param name
     *            The full classpath name of the DatagramConverter. For example:
     *            "amazon.platform.jms.HessianEncoderDecoder" or:
     *            "amazon.platform.clienttoolkit.util.TibcoEncoderDecoder".
     * @throws ClassNotFoundException 
     * @throws IllegalAccessException 
     * @throws InstantiationException 
     */
    void addConverter(String name) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
            DatagramConverter encdec = (DatagramConverter) Class.forName(name)
                    .newInstance();
            encoders_.put(encdec.getFormatName(),encdec);
            decoders_.put(Integer.valueOf(encdec.getMagicBytes()), encdec);
    }
    
    public DatagramConverter getDecoder(byte [] bytes) throws DecodingException {
        if (bytes.length < 8)
            throw new DecodingException(
                    "Message too short to contain magic bytes!");
        int magic = ((bytes[4] << 24) & 0xFF000000)
        + ((bytes[5] << 16) & 0x00FF0000)
        + ((bytes[6] << 8) & 0x0000FF00)
        + ((bytes[7]) & 0x000000FF);
        DatagramConverter result = (DatagramConverter) decoders_.get(Integer.valueOf(magic));
        if (result == null)
            throw new DecodingException("This message is in an unknown wireformat. The magic bytes used were: \"" + magic);

        return result;      
    }
    
    public DatagramConverter getEncoder(String name) throws EncodingException {
        DatagramConverter result;
        if (name == null || name.length() <= 0)
            result = DEFAULT_INSTANCE;
        else 
            result = (DatagramConverter) encoders_.get(name);
        
        if (result == null)
            throw new EncodingException("The specified wireformat: \"" + name +"\" does not exist in your enviroment.");

        return result;      
    }
    
}
