package publisher.debs2016;

import com.google.common.base.Splitter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by sajith on 7/24/16.
 */
public class DataLoaderThread extends Thread{
    private final String fileName;
    private final static Splitter splitter = Splitter.on('|');
    private final LinkedBlockingQueue<Object[]> eventBufferList;
    private final FileType fileType;
    private static final String MINUS_ONE = "-1";

    private long postCount;
    private long postSize;
    private long commentCount;
    private long commentSize;
    private long friendshipCount;
    private long friendshipSize;
    private long likesCount;
    private long likesSize;
    /**
     * The constructor
     *
     * @param fileName the name of the file to be read
     * @param fileType the type of the file to be read
     * @param bufferLimit the size limit of queues
     */
    public DataLoaderThread(String fileName, FileType fileType, int bufferLimit){
        super("Data Loader");
        this.fileName = fileName;
        this.fileType = fileType;
        eventBufferList = new LinkedBlockingQueue<>(bufferLimit);
    }

    public void run() {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName), 10 * 1024 * 1024)){
            String line = bufferedReader.readLine();
            Object[] eventData;
            String user;
            while (line != null) {
                Iterator<String> dataStreamIterator = splitter.split(line).iterator();
                switch(fileType) {
                    case POSTS:
                        String postsTimeStampString = dataStreamIterator.next();
                        if (("").equals(postsTimeStampString)){
                            break;
                        }
                        DateTime postDateTime = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(postsTimeStampString);
                        long postsTimeStamp = postDateTime.getMillis();
                        long postID = Long.parseLong(dataStreamIterator.next());
                        long userID = Long.parseLong(dataStreamIterator.next());
                        String post = dataStreamIterator.next();
                        user = dataStreamIterator.next();
                        eventData = new Object[]{
                                0L,
                                postsTimeStamp,
                                postID,
                                userID,
                                post,
                                user,
                                0L,
                                0L,
                                Constants.POSTS
                        };
                        eventBufferList.put(eventData);
                        postCount++;
                        postSize += 28 + post.length() + user.length();
                        break;
                    case COMMENTS:
                        String commentTimeStampString = dataStreamIterator.next();
                        if (("").equals(commentTimeStampString)){
                            break;
                        }
                        DateTime commentDateTime = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(commentTimeStampString);
                        long commentTimeStamp = commentDateTime.getMillis();
                        long commentID = Long.parseLong(dataStreamIterator.next());
                        userID = Long.parseLong(dataStreamIterator.next());
                        String comment = dataStreamIterator.next();
                        user = dataStreamIterator.next();
                        String commentReplied = dataStreamIterator.next();

                        if(("").equals(commentReplied)){
                            commentReplied = MINUS_ONE;
                        }

                        long commentRepliedId = Long.parseLong(commentReplied);
                        String postCommented = dataStreamIterator.next();

                        if(("").equals(postCommented)){
                            postCommented = MINUS_ONE;
                        }
                        long postCommentedId = Long.parseLong(postCommented);
                        eventData = new Object[]{
                                0L,
                                commentTimeStamp,
                                userID,
                                commentID,
                                comment,
                                user,
                                commentRepliedId,
                                postCommentedId,
                                Constants.COMMENTS
                        };
                        eventBufferList.put(eventData);
                        commentCount++;
                        commentSize += 30 + user.length() + comment.length();
                        break;
                    case FRIENDSHIPS:
                        String friendshipsTimeStampString = dataStreamIterator.next();
                        if (("").equals(friendshipsTimeStampString)){
                            break;
                        }
                        DateTime friendshipDateTime = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(friendshipsTimeStampString);
                        long  friendshipTimeStamp = friendshipDateTime.getMillis();
                        long user1ID = Long.parseLong(dataStreamIterator.next());
                        long user2ID = Long.parseLong(dataStreamIterator.next());
                        eventData = new Object[]{
                                0L,
                                friendshipTimeStamp,
                                user1ID,
                                user2ID,
                                "0",
                                "0",
                                0L,
                                0L,
                                Constants.FRIENDSHIPS
                        };
                        eventBufferList.put(eventData);
                        friendshipCount++;
                        friendshipSize += 30;
                        break;
                    case LIKES:
                        String likeTimeStampString = dataStreamIterator.next(); //e.g., 2010-02-09T04:05:20.777+0000
                        if (("").equals(likeTimeStampString)){
                            break;
                        }
                        DateTime likeDateTime = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(likeTimeStampString);
                        long  likeTimeStamp = likeDateTime.getMillis();
                        userID = Long.parseLong(dataStreamIterator.next());
                        commentID = Long.parseLong(dataStreamIterator.next());
                        eventData = new Object[]{
                                0L,
                                likeTimeStamp,
                                userID,
                                commentID,
                                "0",
                                "0",
                                0L,
                                0L,
                                Constants.LIKES
                        };
                        eventBufferList.put(eventData);
                        likesCount++;
                        likesSize += 30;
                        break;
                }
                line = bufferedReader.readLine();
            }

            System.out.println("Posts=" + postCount + "," + postSize);
            System.out.println("Comments=" + commentCount + "," + commentSize);
            System.out.println("Friendship=" + friendshipCount + "," + friendshipSize);
            System.out.println("Likes=" + likesCount + "," +likesSize);

            Long postsTimeStampLong = -1L;
            Long postID = -1L;
            Long userID = -1L;
            eventData = new Object[]{
                    -1L,
                    postsTimeStampLong,
                    postID,
                    userID,
                    "0",
                    "0",
                    0L,
                    0L,
                    Constants.POSTS
            };
            eventBufferList.put(eventData);

        } catch (Throwable e) {
            e.printStackTrace();
        }


    }

    /**
     *
     * @return the event buffer which has the event data
     */
    public LinkedBlockingQueue<Object[]> getEventBuffer()
    {
        return eventBufferList;
    }

}

