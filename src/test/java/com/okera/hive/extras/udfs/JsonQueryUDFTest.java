package com.okera.hive.extras.udfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.junit.Test;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.Text;

public class JsonQueryUDFTest {

  @Test
  public void jsonQueryTest() throws IOException, HiveException {
      JsonQueryUDF udf = new JsonQueryUDF();

      runAndVerify("[\"a\",\"b\",\"c\"]", ".[1]", "b", udf);

      String example = new String(getClass().getClassLoader().
        getResourceAsStream("samples/example.json").
        readAllBytes());
      runAndVerify(example, 
        ".evidence[] | select(.type == \"IdDocument\") | .images[].processingStatus", 
        "Successful", udf);
    }

    private void runAndVerify(String json, String jq, String expResult, JsonQueryUDF udf)
        throws HiveException {
      Text output = udf.evaluate(json, jq);
      if(expResult != null) {
          assertEquals("jsonQuery() test ", expResult, output.toString());
      } else {
          assertNull("jsonQuery() test ", output);
      }
    }

}