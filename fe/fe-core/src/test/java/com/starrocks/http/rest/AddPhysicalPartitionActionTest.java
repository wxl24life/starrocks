// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.http.rest;

import com.starrocks.http.StarRocksHttpTestCase;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class AddPhysicalPartitionActionTest extends StarRocksHttpTestCase {
    private static final String PATH_URI = "/_add_physical_partition";

    @Test
    public void testMissingDatabase() throws IOException {
        // Test with missing database
        Request request = new Request.Builder()
                .post(RequestBody.create(null, new byte[0]))
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/api/" + "nonexistent_db" + "/" + TABLE_NAME + PATH_URI)
                .build();

        Response response = networkClient.newCall(request).execute();
        JSONObject jsonObject = new JSONObject(response.body().string());
        // Should fail because database doesn't exist
        Assertions.assertEquals("FAILED", jsonObject.get("status"));
        Assertions.assertTrue(jsonObject.getString("msg").contains("Database 'nonexistent_db' does not exist"),
                "Error message should mention database does not exist, got: " + jsonObject.getString("msg"));
    }

    @Test
    public void testMissingTable() throws IOException {
        // Test with missing table
        Request request = new Request.Builder()
                .post(RequestBody.create(null, new byte[0]))
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/api/" + DB_NAME + "/" + "nonexistent_table" + PATH_URI)
                .build();

        Response response = networkClient.newCall(request).execute();
        JSONObject jsonObject = new JSONObject(response.body().string());
        // Should fail because table doesn't exist
        Assertions.assertEquals("FAILED", jsonObject.get("status"));
        Assertions.assertTrue(jsonObject.getString("msg").contains("Table 'nonexistent_table' does not exist"),
                "Error message should mention database does not exist, got: " + jsonObject.getString("msg"));
    }

    @Test
    public void testHashDistributionTableError() throws IOException {
        // Test with existing hash distribution table (should fail)
        Request request = new Request.Builder()
                .post(RequestBody.create(null, new byte[0]))
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/api/" + DB_NAME + "/" + TABLE_NAME + PATH_URI)
                .build();

        Response response = networkClient.newCall(request).execute();
        JSONObject jsonObject = new JSONObject(response.body().string());
        // Should fail because the test table uses hash distribution, not random
        Assertions.assertEquals("FAILED", jsonObject.get("status"));
        // Verify the error message mentions random distribution
        String message = jsonObject.optString("msg", "");
        Assertions.assertTrue(message.contains("random distribution") || message.contains("RANDOM"),
                "Error message should mention random distribution requirement, got: " + message);
    }

    @Test
    public void testInvalidBucketNumber() throws IOException {
        // Test with invalid bucket number (negative)
        Request request = new Request.Builder()
                .post(RequestBody.create(null, new byte[0]))
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/api/" + DB_NAME + "/" + TABLE_NAME + PATH_URI + "?buckets=-1")
                .build();

        Response response = networkClient.newCall(request).execute();
        JSONObject jsonObject = new JSONObject(response.body().string());
        // Should fail because bucket number is invalid
        Assertions.assertEquals("FAILED", jsonObject.get("status"));
    }

    @Test
    public void testInvalidBucketNumberFormat() throws IOException {
        // Test with non-numeric bucket number
        Request request = new Request.Builder()
                .post(RequestBody.create(null, new byte[0]))
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/api/" + DB_NAME + "/" + TABLE_NAME + PATH_URI + "?buckets=abc")
                .build();

        Response response = networkClient.newCall(request).execute();
        JSONObject jsonObject = new JSONObject(response.body().string());
        // Should fail because bucket number format is invalid
        Assertions.assertEquals("FAILED", jsonObject.get("status"));
        String message = jsonObject.optString("msg", "");
        Assertions.assertTrue(message.contains("Invalid bucket number"),
                "Error message should mention invalid bucket number, got: " + message);
    }

    @Test
    public void testBucketNumberExceedsMaximum() throws IOException {
        // Test with bucket number exceeding maximum allowed value
        // The max_bucket_number_per_partition config limits the maximum bucket number
        int exceedingBucketNum = 1000000;  // An unreasonably large number
        Request request = new Request.Builder()
                .post(RequestBody.create(null, new byte[0]))
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/api/" + DB_NAME + "/" + TABLE_NAME + PATH_URI + "?buckets=" + exceedingBucketNum)
                .build();

        Response response = networkClient.newCall(request).execute();
        JSONObject jsonObject = new JSONObject(response.body().string());
        // Should fail because bucket number exceeds maximum
        Assertions.assertEquals("FAILED", jsonObject.get("status"));
        String message = jsonObject.optString("msg", "");
        Assertions.assertTrue(message.contains("exceeds maximum") || message.contains("random distribution"),
                "Error message should mention exceeding maximum or random distribution, got: " + message);
    }

    @Test
    public void testValidBucketNumberWithHashTable() throws IOException {
        // Test with valid bucket number but hash distribution table
        // Should still fail because the table uses hash distribution
        Request request = new Request.Builder()
                .post(RequestBody.create(null, new byte[0]))
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/api/" + DB_NAME + "/" + TABLE_NAME + PATH_URI + "?buckets=10&partition=testPartition")
                .build();

        Response response = networkClient.newCall(request).execute();
        JSONObject jsonObject = new JSONObject(response.body().string());
        // Should fail because the test table uses hash distribution
        Assertions.assertEquals("FAILED", jsonObject.get("status"));
        String message = jsonObject.optString("msg", "");
        Assertions.assertTrue(message.contains("random distribution") || message.contains("RANDOM"),
                "Error message should mention random distribution requirement, got: " + message);
    }

    @Test
    public void testNonOlapTableError() throws IOException {
        // Test with non-OLAP table
        Request request = new Request.Builder()
                .post(RequestBody.create(null, new byte[0]))
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/api/" + DB_NAME + "/" + ES_TABLE_NAME + PATH_URI)
                .build();

        Response response = networkClient.newCall(request).execute();
        JSONObject jsonObject = new JSONObject(response.body().string());
        // Should fail because only OLAP table supports adding physical partition
        Assertions.assertEquals("FAILED", jsonObject.get("status"));
        String message = jsonObject.optString("msg", "");
        Assertions.assertTrue(message.contains("Only OLAP table"),
                "Error message should mention OLAP table requirement, got: " + message);
    }

    @Test
    public void testZeroBucketNumber() throws IOException {
        // Test with zero bucket number (should be valid, means system auto-infer)
        // The request should pass parameter validation but fail on distribution type
        Request request = new Request.Builder()
                .post(RequestBody.create(null, new byte[0]))
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/api/" + DB_NAME + "/" + TABLE_NAME + PATH_URI + "?buckets=0")
                .build();

        Response response = networkClient.newCall(request).execute();
        JSONObject jsonObject = new JSONObject(response.body().string());
        // Should fail because the test table uses hash distribution, not on bucket validation
        Assertions.assertEquals("FAILED", jsonObject.get("status"));
        String message = jsonObject.optString("msg", "");
        // Should not mention bucket number error, but rather distribution type
        Assertions.assertTrue(message.contains("random distribution") || message.contains("RANDOM"),
                "Error message should mention random distribution requirement, got: " + message);
    }

    @Test
    public void testWithPartitionParameter() throws IOException {
        // Test with partition parameter
        Request request = new Request.Builder()
                .post(RequestBody.create(null, new byte[0]))
                .addHeader("Authorization", rootAuth)
                .url(BASE_URL + "/api/" + DB_NAME + "/" + TABLE_NAME + PATH_URI + "?partition=p1")
                .build();

        Response response = networkClient.newCall(request).execute();
        JSONObject jsonObject = new JSONObject(response.body().string());
        // Should fail on distribution type check, not partition validation
        Assertions.assertEquals("FAILED", jsonObject.get("status"));
        String message = jsonObject.optString("msg", "");
        Assertions.assertTrue(message.contains("random distribution") || message.contains("RANDOM"),
                "Error message should mention random distribution requirement, got: " + message);
    }
}
