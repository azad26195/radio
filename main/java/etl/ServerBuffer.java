package etl;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.operation.AssertionLevel;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
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
import org.joda.time.LocalDate;

import org.joda.time.DateTimeComparator;
import java.util.*;

public class ServerBuffer extends BaseOperation implements Buffer
{
  String range="";
  public ServerBuffer()
  {
    super( 1, new Fields("sensorid","range","status") );
  }

  public ServerBuffer( Fields fieldDeclaration )
  {
    super( 1, fieldDeclaration );
  }



  public void operate( FlowProcess flowProcess, BufferCall bufferCall )
  {
    // init the count and sum

    // long sum = 0;

    // get all the current argument values for this grouping

    String startTime = "";
    String currentTime = "";
    String endTime = "";
    String currentStatus = "ON";
    int isStatusChange = 0;
    boolean isFirstTime = true;

    Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();

    TupleEntry currentSensor = new TupleEntry();

    ArrayList<SensorStatus> sensorList = new ArrayList();


    if(arguments.hasNext() & isFirstTime){
      currentSensor = arguments.next();
      startTime =  currentSensor.getString(2);
      currentSensor = arguments.next();
      endTime = currentSensor.getString(2);
      currentTime = startTime;
    }

    while( arguments.hasNext())
    {
      isFirstTime = false;
      

      if(( LocalDate.parse(endTime).getLocalMillis() - LocalDate.parse(currentTime).getLocalMillis() ) < 5000 ){
        currentSensor = arguments.next();
        currentTime = endTime;
        endTime  = currentSensor.getString(2);

        isStatusChange = 0;

      }
      else{

        sensorList.add(new SensorStatus(startTime ,endTime,currentStatus));

        isStatusChange = 1;
      }

      if(isStatusChange){
        currentSensor = arguments.next();
        endTime = currentSensor.getString(2);
       startTime = currentTime;

       isStatusChange = 0;
       currentStatus = "OFF";

     }


   }

   isFirstTime = false;

   Iterator<ArrayList> resultList = sensorList.iterator();

    // while(resultList.hasNext()){
    //   SensorStatus results = resultList.next()
    //   Tuple result = new Tuple(arguments,results.from + " - " + results.to , ;
    //   bufferCall.getOutputCollector().add(result);

    // }
    // // create a Tuple to hold our result values

    // return the result Tuple
 }


}