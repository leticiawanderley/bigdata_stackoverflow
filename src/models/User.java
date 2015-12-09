package models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class User implements WritableComparable<User> {
    private IntWritable userId;
    private Text userName;
    private Text firstPost;
    private Text lastPost;
    private Text creationDate;
   
    public User() {
        set(new IntWritable(), new Text(), new Text(),new Text(), new Text());
    }

    public User(int id, String user, String fstPost, String lstPost, String crtDate) {
        set(new IntWritable(id), new Text(user), new Text(fstPost), new Text(lstPost), new Text(crtDate));
    }

    public User(IntWritable id, Text userName, Text firstPost, Text lastPost, Text creationDate) {
        set(id, userName, firstPost, lastPost, creationDate);
    }

    public void set(IntWritable id, Text userName, Text firstPost, Text lastPost, Text creationDate) {
        this.userId = id;
        this.userName = userName;
        this.firstPost = firstPost;
        this.lastPost = lastPost;
        this.creationDate = creationDate;
    }

    public IntWritable getUserId() {
        return userId;
    }

    public Text getUserName() {
        return userName;
    }

    public Text getFirstPost() {
        return firstPost;
    }

    public Text getLastPost() {
        return lastPost;
    }

    public Text getCreationDate() {
        return creationDate;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        userId.write(out);
        userName.write(out);
        firstPost.write(out);
        lastPost.write(out);
        creationDate.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        userId.readFields(in);
        userName.readFields(in);
        firstPost.readFields(in);
        lastPost.readFields(in);
        creationDate.readFields(in);       
    }

    @Override
    public String toString() {
        return "["+ userId + "," + userName + "," + firstPost + "," + lastPost + "," + creationDate + "]";
    }

    @Override
    public int compareTo(User us) {
        return userId.compareTo(us.getUserId());               
    }
   
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((userId == null) ? 0 : userId.hashCode());
        result = prime * result + ((firstPost == null) ? 0 : firstPost.hashCode());
        result = prime * result + ((lastPost == null) ? 0 : lastPost.hashCode());
        result = prime * result + ((creationDate == null) ? 0 : creationDate.hashCode());
        result = prime * result + ((userName == null) ? 0 : userName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof User) {
            User us = (User) obj;
            return userId.equals(us.getUserId());
        }
        return false;
    }
}
