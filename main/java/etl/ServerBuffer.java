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

  public Date toDateString(String timestampString){

      Long timestamp = new Long(timestampString.trim());
      Date date = new Date(timestamp*1000L); // *1000 is to convert seconds to milliseconds
      // SimpleDateFormat sdf = new SimpleDateFormat("HH:mm"); // the format of your date
      // sdf.setTimeZone(DateTimeZone.UTC); // give a timezone reference for formating (see comment at the bottom
      // Date formattedDate = sdf.format(date);

      return date;

    }

    public String HHMM(String timestampString){
      
    }


  public void operate( FlowProcess flowProcess, BufferCall bufferCall )
  {
    // init the count and sum

    // long sum = 0;

    // get all the current argument values for this grouping

    String startTimeString = "";
    String currentTimeString = "";
    String endTimeString = "";
    String currentStatus = "ON";
    boolean isStatusChange = false;
    boolean isFirstTime = true;

    Date endTime = new Date();
    Date currentTime = new  Date();


    Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();

    TupleEntry currentSensor = new TupleEntry();

    ArrayList<SensorStatus> sensorList = new ArrayList();


    if(arguments.hasNext() & isFirstTime){
      currentSensor = arguments.next();
      startTimeString =  currentSensor.getString(2);
      currentSensor = arguments.next();
      endTimeString = currentSensor.getString(2);
      currentTimeString = startTimeString;
    }

    while( arguments.hasNext())
    {
      isFirstTime = false;



      currentTime =toDateString(currentTimeString);
      endTime = toDateString(endTimeString);
      

      if((endTime.getTime() - currentTime.getTime() ) < 2000){
        currentSensor = arguments.next();
        currentTimeString = endTimeString;
        endTimeString  = currentSensor.getString(2);

        isStatusChange = false;

      }
      else{

        sensorList.add(new SensorStatus(startTimeString ,endTimeString,currentStatus));


        isStatusChange = true;
      }

      if(isStatusChange){
        currentSensor = arguments.next();
        endTimeString = currentSensor.getString(2);
       startTimeString = currentTimeString;

       isStatusChange = false;
       currentStatus = (currentStatus == "OFF")?"ON":"OFF";

     }


   }

   isFirstTime = false;

   Iterator<SensorStatus> resultList = sensorList.iterator();

    while(resultList.hasNext()){
      SensorStatus results = resultList.next();
      System.out.println(results.from);
      Tuple result = new Tuple(currentSensor.getString(1), "", results.status) ;
      bufferCall.getOutputCollector().add(result);

    }
    // // create a Tuple to hold our result values

    // return the result Tuple
 }


}