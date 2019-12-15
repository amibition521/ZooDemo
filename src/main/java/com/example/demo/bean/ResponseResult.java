package com.example.demo.bean;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
@Getter
@Setter
@NoArgsConstructor
public class ResponseResult {
    private int code;
    private String msg;
    private String body;
}
