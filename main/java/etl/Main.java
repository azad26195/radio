package etl;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.operation.AssertionLevel;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.aggregator.Count;
import cascading.operation.FunctionCall;
import cascading.operation.assertion.AssertExpression;
import cascading.operation.text.DateParser;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.PartitionTap;
import cascading.tap.partition.DelimitedPartition;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;
import java.util.Date;

public class Main{
	public static void main( String[] args ){
		
		Properties properties = new Properties();
		AppProps.setApplicationJarClass( properties, Main.class );
		Hadoop2MR1FlowConnector flowConnector = new Hadoop2MR1FlowConnector( properties );

		AppProps.addApplicationTag( properties, "cluster:development" );
		AppProps.setApplicationName( properties, "sensor-status-job" );

	    // Input file
		String inputPath = args[ 0 ];

	    // Output file
		String outputPath = args[ 1 ];

		Hfs inTap = new Hfs( new TextLine(new Fields("offset", "line")), inputPath );

	    // Create a sink tap to write to the Hfs; by default, TextDelimited writes all fields out
		Hfs outTap = new Hfs( new TextDelimited( true, "," ), outputPath, SinkMode.REPLACE );

		Fields tupleField = new Fields("version", "sensorid", "serverts");

		Function splitter = new Splitter(tupleField);

		Pipe processPipe = new Each("processPipe", new Fields("line"), splitter, tupleField);

		Pipe groupByPipe = new Pipe("wc" , processPipe);

		groupByPipe = 	new GroupBy(groupByPipe, new Fields("sensorid"),new Fields("serverts"));

		groupByPipe = new Every(groupByPipe,tupleField,new ServerBuffer(),Fields.RESULTS);

		FlowDef flowDef = FlowDef.flowDef().addSource(processPipe, inTap).addTailSink(groupByPipe,outTap);
		Flow wcFlow = flowConnector.connect(flowDef);
		wcFlow.complete();

	}

}

	

