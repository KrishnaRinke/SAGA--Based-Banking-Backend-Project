package com.bankapp.frauddetectionservice.model;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FraudDetectionResult {

    private boolean fraud;

    private String reason;
}
