package com.amazonaws.services.kinesisanalytics.utils;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

public class AvroSerializationFn<T extends SpecificRecordBase> implements SerializationSchema<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AvroSerializationFn.class);

    @Override
    public byte[] serialize(T record) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = null;

        try {
            oos = new ObjectOutputStream(baos);

            record.writeExternal(oos);
            oos.flush();
            baos.flush();

            return baos.toByteArray();
        } catch (Exception ex) {
            LOG.error("Unable to serialize employee info");
            LOG.error(ex.getMessage());

            return null;
        } finally {
            try {
                if(oos != null) oos.close();
                baos.close();
            } catch (Exception ex) {
                LOG.error("Unable to serialize employee info (while calling close on ByteArrayOutputStream");
                LOG.error(ex.getMessage());

                return null;
            }
        }
    }
}
