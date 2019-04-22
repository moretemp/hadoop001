package day05;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

public class MovieBean implements WritableComparable<MovieBean> {
    private String movie;
    private int rate;
    private Long timeStamp;
    private int uid;


    public String getMovie() {
        return movie;
    }

    public void setMovie(String movie) {
        this.movie = movie;
    }

    public int getRate() {
        return rate;
    }

    public void setRate(int rate) {
        this.rate = rate;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public int getUid() {
        return uid;
    }

    public void setUid(int uid) {
        this.uid = uid;
    }


    @Override
    public String toString() {
        return "Move{" +
                "movie='" + movie + '\'' +
                ", rate=" + rate +
                ", timeStamp=" + timeStamp +
                ", uid=" + uid +
                '}';
    }


    public int compareTo(MovieBean o) {
        return 0;
    }


    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(movie);
        dataOutput.writeInt(rate);
        dataOutput.writeLong(timeStamp);
        dataOutput.writeInt(uid);
    }


    public void readFields(DataInput dataInput) throws IOException {
        movie = dataInput.readUTF();//string类型
        rate = dataInput.readInt();
        timeStamp =dataInput.readLong();
        uid = dataInput.readInt();
    }


}
