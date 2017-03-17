/**
 * Put your copyright and license info here.
 */
package com.Self.After;

import org.apache.hadoop.conf.Configuration;

import com.Self.After.aggregator.meta.AggregationHelper;
import com.Self.After.aggregator.meta.AggregationMetrics;
import com.Self.After.aggregator.meta.AggregationSchema;
import com.Self.After.aggregator.meta.AggregationTypes;
import com.Self.After.row.DataType;
import com.Self.After.row.RowMeta;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="After")
public class  Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
  
    RowMeta rowMeta = new RowMeta();
    rowMeta.addField("publisher", DataType.STRING);
    rowMeta.addField("advertiser", DataType.STRING);
    rowMeta.addField("location", DataType.STRING);
    rowMeta.addField("cost", DataType.LONG);
    rowMeta.addField("impressions", DataType.LONG);
    rowMeta.addField("clicks", DataType.BOOLEAN);
  
    String keys[] = {"publisher"};
    String vals[] = {"cost"};
    AggregationMetrics metrics = new AggregationMetrics(keys,vals, AggregationTypes.MAX);
    
    AggregationHelper helper = new AggregationHelper();
    AggregationSchema schema = null;
    try {
      schema = helper.createAggregationSchema(rowMeta, metrics);
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  
  
    Generator generator = dag.addOperator("Generator", new Generator());
    
    Intermediate intermediate = dag.addOperator("Converter", new Intermediate(rowMeta,metrics,schema));

    Aggregation aggregation = dag.addOperator("Aggregation", new Aggregation(metrics, schema));
    
    ConsoleOutputOperator consoleOutputOperator = dag.addOperator("Console", new ConsoleOutputOperator());

    dag.addStream("Input --> Encoding", generator.outputPort, intermediate.inputPort);
    dag.addStream("Encoding --> Aggregation", intermediate.outputPort, aggregation.inputPort);
    dag.addStream("Aggregation --> Result", aggregation.outputPort, consoleOutputOperator.input);
  }
}
