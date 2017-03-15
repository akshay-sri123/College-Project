/**
 * Put your copyright and license info here.
 */
package com.Self.Before;

import com.Self.Before.main.Generator;
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
      Generator generator = dag.addOperator("Generator", new Generator());
      ConsoleOutputOperator outputOperator = dag.addOperator("Console", new ConsoleOutputOperator());

      dag.addStream("Generated data", generator.outputPort, outputOperator.input);
  }
}
