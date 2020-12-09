package topInningsByRunsAndBalls;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

//public class CompositeKey implements WritableComparable<CompositeKey> {
//    private String name;
//    private String score;
//    public CompositeKey(){
//        super();
//    }
//    public CompositeKey(String name, String score)
//    {
//        super();
//        this.name=name;
//        this.score=score;
//    }
//    public int compareTo(CompositeKey o) {
//        int result = this.name.compareTo(o.name); 
//        if(result==0){
//            return this.score.compareTo(o.score);
//        }         
//        return result;
//    }
//    public void write(DataOutput out) throws IOException {
//        out.writeUTF(name);
//        out.writeUTF(score);
//    }
//    public void readFields(DataInput in) throws IOException {
//        name=in.readUTF();
//        score=in.readUTF();
//    }
//	public String getName() {
//		return name;
//	}
//	public void setName(String name) {
//		this.name = name;
//	}
//	public String getScore() {
//		return score;
//	}
//	public void setScore(String score) {
//		this.score = score;
//	}
//	@Override
//	public String toString()
//	{
//	    return name + "," +score;
//	}
//
//}
public class CompositeKey implements WritableComparable<CompositeKey> {
    private String ballsFaced;
    private String score;
    public CompositeKey(){
        super();
    }
    public CompositeKey(String score, String ballsFaced)
    {
        super();
        this.ballsFaced=ballsFaced;
        this.score=score;
    }
    public int compareTo(CompositeKey o) {
        int result = this.ballsFaced.compareTo(o.ballsFaced); 
        if(result==0){
            return this.score.compareTo(o.score);
        }         
        return result;
    }
    public void write(DataOutput out) throws IOException {
        out.writeUTF(ballsFaced);
        out.writeUTF(score);
    }
    public void readFields(DataInput in) throws IOException {
    	ballsFaced=in.readUTF();
        score=in.readUTF();
    }

	public String getBallsFaced() {
		return ballsFaced;
	}
	public void setBallsFaced(String ballsFaced) {
		this.ballsFaced = ballsFaced;
	}
	public String getScore() {
		return score;
	}
	public void setScore(String score) {
		this.score = score;
	}
	@Override
	public String toString()
	{
	    return score + "(" +ballsFaced+")";
	}

}