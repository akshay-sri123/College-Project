/**
 * Put your copyright and license info here.
 */
package com.Self.After;

import com.Self.After.aggregator.meta.AggregationHelper;
import com.Self.After.aggregator.meta.AggregationMetrics;
import com.Self.After.aggregator.meta.AggregationSchema;
import com.Self.After.aggregator.meta.AggregationTypes;
import com.Self.After.row.DataType;
import com.Self.After.row.RowMeta;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.lib.io.ConsoleOutputOperator;

import java.util.List;

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
    String vals[] = {"impressions"};
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
    
    Intermediate intermediate = dag.addOperator("Intermediate", new Intermediate(rowMeta, metrics, schema));
    try {intermediate.init();}
    catch (Exception e) {e.printStackTrace();}
    
    Aggregation aggregation = dag.addOperator("Aggregation", new Aggregation(metrics,schema));
    
    Output output = dag.addOperator("Output", new Output(schema));
    
    ConsoleOutputOperator consoleOutputOperator = dag.addOperator("Console", new ConsoleOutputOperator());
    
    dag.addStream("First", generator.outputPort, intermediate.inputPort);
    dag.addStream("Second", intermediate.outputPort, aggregation.inputPort);
    dag.addStream("Third", aggregation.outputPort, output.inputPort);
    dag.addStream("Fourth", output.outputPort, consoleOutputOperator.input);
  
  }
}
