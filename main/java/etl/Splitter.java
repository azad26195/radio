package etl;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.operation.AssertionLevel;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.assertion.AssertExpression;
import cascading.operation.regex.RegexParser;
import cascading.operation.text.DateParser;
import cascading.pipe.Each;
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
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;
import java.util.Date;
import java.text.SimpleDateFormat;
import org.joda.time.DateTime;
import java.util.*;
public class  Splitter extends BaseOperation implements Function
	{
		public Splitter(Fields fieldDeclaration) {
			super(1,fieldDeclaration );
		}
		
		public  void operate( FlowProcess flowProcess, FunctionCall functionCall )
		{
    // get the arguments TupleEntry
			TupleEntry arguments = functionCall.getArguments();
			String[] tokens = arguments.getString(0).split(",");
			// Long timestamp = new Long(tokens[2].trim());
			// Date date = new Date(timestamp*1000L); // *1000 is to convert seconds to milliseconds
			// SimpleDateFormat sdf = new SimpleDateFormat("HH:mm"); // the format of your date
			// sdf.setTimeZone(TimeZone.getTimeZone("GMT")); // give a timezone reference for formating (see comment at the bottom
			// String formattedDate = sdf.format(date);

			// Timestamp timestamp = new Timestamp(Long.parseLong(tokens[2]));
			// Date date = new Date(timestamp.getTime());

    // create a Tuple to hold our result values
			// Tuple result = new Tuple();
			Tuple result = new Tuple();

			result.add(tokens[0]);
			result.add(tokens[1]);

			result.add(tokens[2]);

    // insert some values into the result Tuple

    // return the result Tuple
			functionCall.getOutputCollector().add( result );
		}

		

	}