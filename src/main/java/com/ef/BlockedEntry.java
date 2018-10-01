package com.ef;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class BlockedEntry {
    private String ip;
    private String comment;
}
