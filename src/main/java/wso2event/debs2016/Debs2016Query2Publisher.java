package wso2event.debs2016;

import wso2event.Publishable;
import wso2event.ResearchEventPublisher;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by sajith on 7/25/16.
 */
public class Debs2016Query2Publisher extends Publishable {

    public Debs2016Query2Publisher() {
        super("inStreamDebs2016q2:1.0.0", Constants.DATA_FILE_PATH);
    }

    @Override
    public void startPublishing() {
        LinkedBlockingQueue<Object[]> eventBufferList [] = new LinkedBlockingQueue[3];


        //Friendships
        DataLoaderThread dataLoaderThreadFriendships = new DataLoaderThread(getDataFilePath() + File.separator + Constants.FRIENDSHIPE_FILE_NAME,
                FileType.FRIENDSHIPS, Constants.BUFFER_LIMIT);

        //Comments
        DataLoaderThread dataLoaderThreadComments = new DataLoaderThread(getDataFilePath() + File.separator + Constants.COMMENT_FILE_NAME,
                FileType.COMMENTS, Constants.BUFFER_LIMIT);

        //Likes
        DataLoaderThread dataLoaderThreadLikes = new DataLoaderThread(getDataFilePath() + File.separator + Constants.LIKES_FILE_NAME,
                FileType.LIKES, Constants.BUFFER_LIMIT);

        eventBufferList[0] = dataLoaderThreadFriendships.getEventBuffer();
        eventBufferList[1] = dataLoaderThreadComments.getEventBuffer();
        eventBufferList[2] = dataLoaderThreadLikes.getEventBuffer();

        OrderedEventSenderThreadQuery2 orderedEventSenderThread = new OrderedEventSenderThreadQuery2(eventBufferList, 1 , 1);

        dataLoaderThreadFriendships.start();
        dataLoaderThreadComments.start();
        dataLoaderThreadLikes.start();

        orderedEventSenderThread.start();


    }

    class OrderedEventSenderThreadQuery2 extends Thread {
        private final LinkedBlockingQueue<Object[]>[] eventBufferList;
        public boolean doneFlag = false;
        long k;
        long duration;

        public OrderedEventSenderThreadQuery2(LinkedBlockingQueue<Object[]> eventBuffer[], int k, long duration) {
            super("Event Sender Query 2");
            this.eventBufferList = eventBuffer;
            this.k = k;
            this.duration = duration * 1000;
        }

        public void run(){
            Object[] friendshipEvent = null;
            Object[] commentEvent = null;
            Object[] likeEvent = null;
            long startTime;
            long systemCurrentTime;
            boolean firstEvent = true;
            int flag = Constants.NO_EVENT;
            boolean friendshipLastEventArrived = false;
            boolean commentsLastEventArrived = false;
            boolean likesLastEventArrived = false;

            while (true) {
                try {
                    if (firstEvent) {
                        Object[] finalFriendshipEvent = new Object[]{
                                0L,
                                -1L,
                                0L,
                                0L,
                                "0",
                                "0",
                                (long)duration,
                                (long)k,
                                0,
                        };
                        systemCurrentTime = System.currentTimeMillis();
                        finalFriendshipEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD] = systemCurrentTime;
                        ResearchEventPublisher.publishEvent(finalFriendshipEvent, getStreamId());
                        Date startDateTime = new Date();
                        startTime = startDateTime.getTime();
                        SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd.hh:mm:ss-a-zzz");
                        System.out.println("Starting the experiment at : " + startTime + "--" + ft.format(startDateTime));
                        firstEvent = false;
                    }

                    try {
                        if (flag == Constants.FRIENDSHIPS) {
                            if (!friendshipLastEventArrived) {
                                friendshipEvent = eventBufferList[Constants.FRIENDSHIPS].take();
                                long lastEvent = (Long) friendshipEvent[0];
                                if (lastEvent == -1L) {
                                    friendshipEvent = null;
                                    friendshipLastEventArrived = true;
                                }
                            }
                        } else if (flag == 1) {
                            if (!commentsLastEventArrived) {
                                commentEvent = eventBufferList[Constants.COMMENTS].take();
                                long lastEvent = (Long) commentEvent[0];
                                if (lastEvent == -1L) {

                                    commentEvent = null;
                                    commentsLastEventArrived = true;
                                }
                            }
                        } else if (flag == 2) {
                            if (!likesLastEventArrived) {
                                likeEvent = eventBufferList[Constants.LIKES].take();
                                long lastEvent = (Long) likeEvent[0];

                                if (lastEvent == -1L) {
                                    likeEvent = null;
                                    likesLastEventArrived = true;
                                }
                            }
                        } else {

                            if (!friendshipLastEventArrived) {
                                friendshipEvent = eventBufferList[Constants.FRIENDSHIPS].take();
                                long lastEvent = (Long) friendshipEvent[0];
                                if (lastEvent == -1L) {
                                    friendshipEvent = null;
                                    friendshipLastEventArrived = true;
                                }
                            }
                            if (!commentsLastEventArrived) {
                                commentEvent = eventBufferList[Constants.COMMENTS].take();
                                long lastEvent = (Long) commentEvent[0];
                                if (lastEvent == -1L) {

                                    commentEvent = null;
                                    commentsLastEventArrived = true;
                                }

                            }
                            if (!likesLastEventArrived) {
                                likeEvent = eventBufferList[Constants.LIKES].take();
                                long lastEvent = (Long) likeEvent[0];
                                if (lastEvent == -1L) {
                                    likeEvent = null;
                                    likesLastEventArrived = true;
                                }
                            }
                        }

                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }

                    long timestampFriendship;
                    long timestampComment;
                    long timestampLike;

                    if (friendshipEvent == null) {
                        timestampFriendship = Long.MAX_VALUE;
                    } else {
                        timestampFriendship = (Long) friendshipEvent[Constants.EVENT_TIMESTAMP_FIELD];
                    }

                    if (commentEvent == null) {
                        timestampComment = Long.MAX_VALUE;
                    } else {
                        timestampComment = (Long) commentEvent[Constants.EVENT_TIMESTAMP_FIELD];
                    }

                    if (likeEvent == null) {
                        timestampLike = Long.MAX_VALUE;
                    } else {
                        timestampLike = (Long) likeEvent[Constants.EVENT_TIMESTAMP_FIELD];
                    }

                    if (timestampFriendship <= timestampComment && timestampFriendship <= timestampLike && timestampFriendship != Long.MAX_VALUE) {
                        systemCurrentTime = System.currentTimeMillis();
                        friendshipEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD] = systemCurrentTime;
                        ResearchEventPublisher.publishEvent(friendshipEvent, getStreamId());
                        flag = Constants.FRIENDSHIPS;
                    } else if (timestampComment <= timestampFriendship && timestampComment <= timestampLike && timestampComment != Long.MAX_VALUE) {
                        systemCurrentTime = System.currentTimeMillis();
                        commentEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD] = systemCurrentTime; //This corresponds to the iij_timestamp
                        ResearchEventPublisher.publishEvent(commentEvent, getStreamId());
                        flag = Constants.COMMENTS;
                    } else if (timestampLike != Long.MAX_VALUE) {
                        systemCurrentTime = System.currentTimeMillis();
                        likeEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD] = systemCurrentTime; //This corresponds to the iij_timestamp
                        ResearchEventPublisher.publishEvent(likeEvent, getStreamId());
                        flag = Constants.LIKES;
                    }

                    if (friendshipEvent == null && commentEvent == null && likeEvent == null) {
                        systemCurrentTime = System.currentTimeMillis();
                        Object[] finalFriendshipEvent = new Object[]{
                                0L,
                                -2L,
                                0L,
                                0L,
                                0L,
                                0L,
                                0L,
                                0L,
                                0,
                        };

                        finalFriendshipEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD] = systemCurrentTime;
                        ResearchEventPublisher.publishEvent(finalFriendshipEvent, getStreamId());
                        doneFlag = true;
                        break;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
