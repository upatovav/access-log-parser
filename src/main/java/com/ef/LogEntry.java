package com.ef;

import lombok.Data;
import org.springframework.format.annotation.DateTimeFormat;

import java.time.LocalDateTime;

@Data
public class LogEntry {

    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS")
    private LocalDateTime entryDate;
    private String ip;
    private String request;
    private String status;
    private String userAgent;
}
