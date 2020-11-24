package com.okera.hive.extras.udfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.junit.Test;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

public class JsonQueryUDFTest {

  @Test
  public void jsonQueryTest() throws IOException, HiveException {
      JsonQueryUDF udf = new JsonQueryUDF();

      ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
      ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
      ObjectInspector[] arguments = { valueOI1, valueOI2 };
  
      udf.initialize(arguments);

      runAndVerify("[\"a\",\"b\",\"c\"]", ".[1]", "b", udf);

      String example = new String(getClass().getClassLoader().
        getResourceAsStream("samples/example.json").
        readAllBytes());
      runAndVerify(example, 
        ".evidence[] | select(.type == \"IdDocument\") | .images[].processingStatus", 
        "Successful", udf);
    }

    private void runAndVerify(String json, String jq, String expResult, GenericUDF udf)
        throws HiveException {
      DeferredObject valueObj1 = new DeferredJavaObject(new Text(json));
      DeferredObject valueObj2 = new DeferredJavaObject(new Text(jq));
      DeferredObject[] args = { valueObj1, valueObj2 };
      Object output = udf.evaluate(args);
      if(expResult != null) {
          assertEquals("jsonQuery() test ", expResult, output.toString());
      } else {
          assertNull("jsonQuery() test ", output);
      }
    }

}