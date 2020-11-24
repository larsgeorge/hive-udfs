package com.okera.hive.extras.udfs;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;

import net.thisptr.jackson.jq.BuiltinFunctionLoader;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.Versions;
import net.thisptr.jackson.jq.exception.JsonQueryException;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

/**
 * Provides access to JSON Query (JQ), see
 * https://stedolan.github.io/jq/tutorial/
 */
@Description(name = "jq", value = "Queries the given JSON with JQ, result is returned as a string", extended = "Example:\n"
    + "  jq(json, '.rows[0]')" + "Arguments:\n" + "  jq(<json_data>, <jq_expression>)\n"
    + "    <json_data>     - a valid JSON structure stored as a STRING\n"
    + "    <jq_expression> - the jq expression to apply to the <json_data>\n")
public class JsonQueryUDF extends GenericUDF {
  private final JsonFactory jsonFactory = new JsonFactory();
  private final ObjectMapper objectMapper = new ObjectMapper(jsonFactory);

  private StringObjectInspector jsonStringInspector, jqStringInspector;
  private Scope rootScope;
  private JsonQuery constQuery;

  public JsonQueryUDF() {
    // First of all, you have to prepare a Scope which s a container of
    // built-in/user-defined functions and variables.
    rootScope = Scope.newEmptyScope();
    // Use BuiltinFunctionLoader to load built-in functions from the classpath.
    BuiltinFunctionLoader.getInstance().loadFunctions(Versions.JQ_1_5, rootScope);

    // After this initial setup, rootScope should not be modified (via
    // Scope#setValue(...), Scope#addFunction(...), etc.) so that it can be shared
    // (in a read-only manner) across mutliple threads because you want to avoid
    // heavy lifting of loading built-in functions every time which involves
    // file system operations and a lot of parsing.

    // Instead of modifying the rootScope directly, you can create a child Scope.
    // This is especially useful when you want to use variables or functions
    // that is only local to the specific execution context (such as a thread,
    // request, etc).
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    try {
      // Creating a child Scope is a very light-weight operation that just allocates a
      // Scope and sets
      // one of its fields to point to the given parent scope. It's totally okay to
      // create a child Scope
      // per every apply() invocations if you need to do so.
      Scope childScope = Scope.newChildScope(rootScope);
      // Scope#setValue(...) sets a custom variable that can be used from jq
      // expressions. This variable is local to the
      // childScope and cannot be accessed from the rootScope. The rootScope will not
      // be modified by this call.
      // childScope.setValue("param", IntNode.valueOf(42));

      // JsonQuery#compile(...) parses and compiles a given expression. The resulting
      // JsonQuery instance
      // is immutable and thread-safe. It should be reused as possible if you
      // repeatedly use the same expression.
      JsonQuery jsonQuery = constQuery;
      if (constQuery == null) {
        String jq = this.jqStringInspector.getPrimitiveJavaObject(arguments[1].get());
        jsonQuery = JsonQuery.compile(jq, Versions.JQ_1_5);
      }

      // You need a JsonNode to use as an input to the JsonQuery. There are many ways
      // you can grab a JsonNode.
      // In this example, we just parse a JSON text into a JsonNode.
      String json = this.jsonStringInspector.getPrimitiveJavaObject(arguments[0].get());
      JsonNode in = objectMapper.readTree(json);

      // Finally, JsonQuery#apply(...) executes the query with given input and
      // produces 0, 1 or more JsonNode.
      // The childScope will not be modified by this call because it internally
      // creates a child scope as necessary.
      final List<JsonNode> out = new ArrayList<JsonNode>();
      jsonQuery.apply(childScope, in, out::add);
      if (out.size() == 1 && out.get(0) instanceof TextNode) {
        return ((TextNode) out.get(0)).textValue();
      } else {
        return out.stream()
          .map(n -> String.valueOf(n))
          .collect(Collectors.joining());
      }
    } catch (JsonQueryException jqe) {
      throw new HiveException(jqe);
    } catch (JsonMappingException jme) {
      throw new HiveException(jme);
    } catch (JsonProcessingException jpe) {
      throw new HiveException(jpe);
    } catch (NullPointerException npe) {
      return null;
    }
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException("This UDF takes exactly 2 arguments.");
    }
    for (int i = 0; i < arguments.length; i++) {
      ObjectInspector oi = arguments[i];
      Category category = oi.getCategory();
      PrimitiveCategory primitive = ((PrimitiveObjectInspector) oi).getPrimitiveCategory();
      if (!category.equals(Category.PRIMITIVE) || primitive != PrimitiveCategory.STRING) {
        throw new UDFArgumentException("This UDF only except STRING parameters.");
      }
    }
    // Remember the two object inspectors to access the actual function parameters
    jsonStringInspector = (StringObjectInspector) arguments[0];
    jqStringInspector = (StringObjectInspector) arguments[1];
    // Check if the jq expression is fixed and remember it for later
    ObjectInspector rightArg = arguments[1];
    if (rightArg instanceof ConstantObjectInspector) {
      String jq = ((ConstantObjectInspector) rightArg).getWritableConstantValue().toString();
      try {
        constQuery = JsonQuery.compile(jq, Versions.JQ_1_5);
      } catch (JsonQueryException e) {
        throw new UDFArgumentException("Failed to compile jq expression.");
      }
    }
    // Return a STRING primitive since that is what this UDF returns
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public String getDisplayString(String[] arg0) {
    return "jq(" + arg0[0] + ")";
  }
}
