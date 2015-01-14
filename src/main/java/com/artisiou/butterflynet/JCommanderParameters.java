package com.artisiou.butterflynet;

import com.beust.jcommander.Parameter;

public class JCommanderParameters {
    @Parameter(names = {"-config"}, description = "Config JSON file", required = true)
    public String config;
}