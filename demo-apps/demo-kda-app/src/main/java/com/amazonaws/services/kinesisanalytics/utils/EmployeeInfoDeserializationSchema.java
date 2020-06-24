package com.amazonaws.services.kinesisanalytics.utils;

import com.amazonaws.services.kinesisanalytics.payloads.EmployeeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class EmployeeInfoDeserializationSchema implements KinesisDeserializationSchema<EmployeeInfo> {
    @Override
    public EmployeeInfo deserialize(byte[] bytes,
                                    String s,
                                    String s1,
                                    long l,
                                    String s2,
                                    String s3) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(in);
        EmployeeInfo ei = new EmployeeInfo();
        ei.readExternal(ois);

        in.close();
        ois.close();

        return ei;
    }

    @Override
    public TypeInformation<EmployeeInfo> getProducedType() {
        return TypeExtractor.getForClass(EmployeeInfo.class);
    }
}
