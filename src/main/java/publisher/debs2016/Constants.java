package publisher.debs2016;

/**
 * Created by sajith on 7/24/16.
 */
public  class Constants {
    public static final int FRIENDSHIPS = 0;
    public static final int COMMENTS = 1;
    public static final int LIKES = 2;
    public static final int POSTS = 0; //Having the same value as FRIENDSHIPS is fine because Query1 and Query2 are running separately.
    public static final int NO_EVENT = 4;
    public static final int EVENT_TIMESTAMP_FIELD = 1;
    public static final int INPUT_INJECTION_TIMESTAMP_FIELD = 0;
    public static final int BUFFER_LIMIT = 1000;
    public static final String DATA_FILE_PATH = "/home/sajith/research/debs-2016/debs2016/data";
    public static final String POST_FILE_NAME = "posts.dat";
    public static final String COMMENT_FILE_NAME = "comments.dat";
    public static final String FRIENDSHIPE_FILE_NAME = "friendships.dat";
    public static final String LIKES_FILE_NAME = "likes.dat";


}
