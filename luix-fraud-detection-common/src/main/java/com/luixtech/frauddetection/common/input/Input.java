package com.luixtech.frauddetection.common.input;

import com.luixtech.frauddetection.common.IngestionTimeAssignable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Input implements IngestionTimeAssignable {

    /**
     * ID of the record
     */
    private String              recordId;
    /**
     * Input record data which can be a nested structure
     */
    private Map<String, Object> record;
    /**
     * Created timestamp of the input record, unit: ms
     */
    public  long                createdTime;
    /**
     * Timestamp of ingestion into the flink input source, unit: ms
     */
    private Long                ingestionTime;

    @Override
    public void setIngestionTime(Long timestamp) {
        this.ingestionTime = timestamp;
    }

    @Override
    public Long getIngestionTime() {
        return this.ingestionTime;
    }
}