package etl;

public class SensorStatus{
	public String from = "";
	public String to = "";
	public String status = "";
	public Long diff;

	public  SensorStatus(String from,String to, Long diff, String status){
	 	this.from = from;
		this.to = to;
		this.status = status;
		this.diff = diff;
	}

}