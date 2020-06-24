/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.amazonaws.services.kinesisanalytics.sources;

import com.amazonaws.services.kinesisanalytics.payloads.EmployeeInfo;
import com.github.javafaker.Faker;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.concurrent.ThreadLocalRandom;

public class RandEmployeeInfoSource implements SourceFunction<EmployeeInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(RandEmployeeInfoSource.class);

    private static final Long WATERMARK_GEN_PERIOD = 35L;
    private static final Long SLEEP_PERIOD = 2L;

    private transient Faker _faker;
    private transient SimpleDateFormat _sdfr;

    private volatile boolean isRunning = true;

    public RandEmployeeInfoSource() {
        initIfNecessary();
    }

    @Override
    public void run(SourceContext<EmployeeInfo> sourceContext) throws Exception {
        while(isRunning) {
            EmployeeInfo ei = getNextEmployee();

            sourceContext.collectWithTimestamp(ei, ei.getEventtimestamp());

            if(ei.getEventtimestamp() % WATERMARK_GEN_PERIOD == 0) {
                sourceContext.emitWatermark(new Watermark(ei.getEventtimestamp()));
            }

            Thread.sleep(SLEEP_PERIOD);
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }

    private void initIfNecessary() {
        if(_faker == null) _faker = new Faker();
        if(_sdfr == null) _sdfr = new SimpleDateFormat("yyyy-MM-dd");
    }

    private EmployeeInfo getNextEmployee() {
        initIfNecessary();

        EmployeeInfo ei = new EmployeeInfo();

        ei.setCompanyid(ThreadLocalRandom.current().nextLong(5));
        ei.setEmployeeid(ThreadLocalRandom.current().nextLong(50));
        ei.setMname(_faker.name().fullName());
        ei.setDob(_sdfr.format(_faker.date().birthday()));
        ei.setEventtimestamp(System.currentTimeMillis());

        return ei;
    }
}