/**
 * Put your copyright and license info here.
 */
package com.Self.Before;

import com.Self.Before.main.Generator;
import com.Self.Before.main.Aggregation;
import com.Self.Before.main.Result;
import com.Self.Before.other.AggregatorById;
import com.Self.Before.other.AggregatorSet;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="Before")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    AggregatorSet aggregatorSet = new AggregatorSet();
    for (int i = 1; i <= 7; i++) {
      AggregatorById aggregator = new AggregatorById(i);
      aggregatorSet.addAggregator(aggregator);
    }

    AggregatorById aggregator = new AggregatorById(i);
    aggregatorSet.addAggregator(aggregator);

    Generator generator = dag.addOperator("Generator", new Generator());

    Aggregation intermediate = dag.addOperator("Aggregation", new Aggregation(aggregatorSet));

    Result result = dag.addOperator("Result", new Result());

    ConsoleOutputOperator consoleOutputOperator = dag.addOperator("Console", new ConsoleOutputOperator());

    dag.addStream("Generate data", generator.outputPort, intermediate.inputPort);
    dag.addStream("Perform agg", intermediate.outputPort, result.inputPort);
    dag.addStream("Result", result.outputPort, consoleOutputOperator.input);
  }
}
