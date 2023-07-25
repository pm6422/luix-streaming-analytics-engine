/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.luixtech.frauddetection.flinkjob.input;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InputConfig {

    private final Map<InputParam<?>, Object> values = new HashMap<>();

    public <T> void put(InputParam<T> key, T value) {
        values.put(key, value);
    }

    public <T> T get(InputParam<T> key) {
        return key.getType().cast(values.get(key));
    }

    public <T> InputConfig(
            Parameters inputParams,
            List<InputParam<String>> stringInputParams,
            List<InputParam<Integer>> intInputParams,
            List<InputParam<Boolean>> boolInputParams) {
        overrideDefaults(inputParams, stringInputParams);
        overrideDefaults(inputParams, intInputParams);
        overrideDefaults(inputParams, boolInputParams);
    }

    public static InputConfig fromParameters(Parameters parameters) {
        return new InputConfig(
                parameters, Parameters.STRING_INPUT_PARAMS, Parameters.INT_INPUT_PARAMS, Parameters.BOOL_INPUT_PARAMS);
    }

    private <T> void overrideDefaults(Parameters inputParams, List<InputParam<T>> params) {
        for (InputParam<T> inputParam : params) {
            put(inputParam, inputParams.getOrDefault(inputParam));
        }
    }
}
