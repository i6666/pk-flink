package avro;

import example.avro.User;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class AvroTest {

    @Test
    public void dataFileWriterTest1() throws IOException {
        User user1 = new User();
        user1.setFavoriteColor("red");
        user1.setName("张三");
        user1.setFavoriteNumber(999);

        User user2 = new User("李四",333,null);

        User user3 = User.newBuilder()
                .setFavoriteColor("")
                .setName("地方")
                .setFavoriteColor("白白")
                .setFavoriteNumber(3433)
                .build();

        DatumWriter<User> datumWriter = new SpecificDatumWriter<>();
        DataFileWriter<User> userDataFileWriter = new DataFileWriter<User>(datumWriter);
        userDataFileWriter.create(user1.getSchema(),new File("src/main/avro/user.avro"));
        userDataFileWriter.append(user1);
        userDataFileWriter.append(user2);
        userDataFileWriter.append(user3);
        userDataFileWriter.close();

    }


    @Test
    public void dataFileReaderTest2() throws IOException {
        SpecificDatumReader<User> datumReader = new SpecificDatumReader<>();
        DataFileReader<User> userDataFileReader = new DataFileReader<User>(new File("src/main/avro/user.avro"),datumReader);
        User user;
        while (userDataFileReader.hasNext()){
            user = userDataFileReader.next();
            System.out.println(user);
        }

    }
}
