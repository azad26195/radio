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
import org.joda.time.DateTimeZone;
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

  public Date toDateString(String timestampString) {
    return new Date(new Long(timestampString.trim() + "000"));
  }

  public DateTime toDateTime(String timestampString){

      Long timestamp = new Long(timestampString.trim() + "000");
      return new DateTime(timestamp,DateTimeZone.UTC); // *1000 is to convert seconds to milliseconds
      // SimpleDateFormat sdf = new SimpleDateFormat("HH:mm"); // the format of your date
      // sdf.setTimeZone(DateTimeZone.UTC); // give a timezone reference for formating (see comment at the bottom
      // Date formattedDate = sdf.format(date);

      // return date;

    }

    public String toHHMM(String timestampString){
      SimpleDateFormat sdf = new  SimpleDateFormat("HH:MM:ss a");
      sdf.setTimeZone(TimeZone.getTimeZone("GMT+5.30"));
      DateTime date = toDateTime(timestampString.trim());

      return date.getHourOfDay() + ":" + date.getMinuteOfDay();    // return sdf.format(toDateString(timestampString.trim()));
    }


  public void operate( FlowProcess flowProcess, BufferCall bufferCall )
  {
    // init the count and sum

    // long sum = 0;

    // get all the current argument values for this grouping

    String i_startTimeString = "";
    String p_currentTimeString = "";
    String c_endTimeString = "";
    Long diff = 0L;
    Date endTime = new Date();
    Date currentTime = new  Date();


    Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();

    TupleEntry currentSensor = new TupleEntry();

    ArrayList<SensorStatus> sensorList = new ArrayList();


    // if(arguments.hasNext() & isFirstTime){
    //   currentSensor = arguments.next();
    //   i_startTimeString =  currentSensor.getString(2);
    //   currentSensor = arguments.next();
    //   c_endTimeString = currentSensor.getString(2);
    //   p_currentTimeString = i_startTimeString;
    // }

      currentSensor = arguments.next();
      i_startTimeString = currentSensor.getString(2);
      p_currentTimeString = i_startTimeString;
    while( arguments.hasNext())
    {

      currentSensor = arguments.next();
      c_endTimeString = currentSensor.getString(2);
      currentTime =toDateString(p_currentTimeString);
      endTime = toDateString(c_endTimeString);
        // System.out.println(endTime.getTime() - currentTime.getTime())
      diff = endTime.getTime() - currentTime.getTime();
      if(diff > 2000 * 60){

        sensorList.add(new SensorStatus(toHHMM(i_startTimeString),toHHMM(p_currentTimeString), diff,"ON"));
        sensorList.add(new SensorStatus(toHHMM(p_currentTimeString),toHHMM(c_endTimeString), diff,"OFF"));
        i_startTimeString = c_endTimeString;
        p_currentTimeString=i_startTimeString;

      }
      else{

        p_currentTimeString = c_endTimeString;
      }


     // f isFirstTime = false;



      

     //  if((endTime.getTime() - currentTime.getTime() ) < 2000){
     //    currentSensor = arguments.next();
     //    p_currentTimeString = c_endTimeString;
     //    c_endTimeString  = currentSensor.getString(2);

     //    isStatusChange = false;

     //  }
     //  else{

     //    sensorList.add(new SensorStatus(i_startTimeString ,c_endTimeString,currentStatus));


     //    isStatusChange = true;
     //  }

     //  if(isStatusChange){
     //    currentSensor = arguments.next();
     //    c_endTimeString = currentSensor.getString(2);
     //   i_startTimeString = p_currentTimeString;

     //   isStatusChange = false;
     //   currentStatus = (currentStatus == "OFF")?"ON":"OFF";

     // }


   }

   sensorList.add(new SensorStatus(toHHMM(i_startTimeString),toHHMM(c_endTimeString), diff,"ON"));


   // isFirstTime = false;

   Iterator<SensorStatus> resultList = sensorList.iterator();

    while(resultList.hasNext()){
      SensorStatus results = resultList.next();
      System.out.println(results.from);
      Tuple result = new Tuple(currentSensor.getString(1),results.from + "-" + results.to + " \tDiff " + results.diff, results.status) ;
      bufferCall.getOutputCollector().add(result);

    }
    // // create a Tuple to hold our result values

  // return the result Tuple
 }


}