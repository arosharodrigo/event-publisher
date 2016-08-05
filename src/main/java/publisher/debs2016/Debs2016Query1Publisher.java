package publisher.debs2016;

import publisher.Publishable;
import publisher.ResearchEventPublisher;

import java.io.File;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by sajith on 7/24/16.
 */
public class Debs2016Query1Publisher extends Publishable {

    public Debs2016Query1Publisher() {
        super("inStreamDebs2016q1:1.0.0", Constants.DATA_FILE_PATH);
    }

    public void startPublishing() {
        // Loading posts
        DataLoaderThread dataLoaderThreadPosts = new DataLoaderThread(getDataFilePath() + File.separator + Constants.POST_FILE_NAME,
                FileType.POSTS, Constants.BUFFER_LIMIT);

        // Loading Comments
        DataLoaderThread dataLoaderThreadComments = new DataLoaderThread(getDataFilePath() + File.separator + Constants.COMMENT_FILE_NAME,
                FileType.COMMENTS, Constants.BUFFER_LIMIT);

        LinkedBlockingQueue<Object[]> eventBufferList[] = new LinkedBlockingQueue[2];

        eventBufferList[Constants.POSTS] = dataLoaderThreadPosts.getEventBuffer();
        eventBufferList[Constants.COMMENTS] = dataLoaderThreadComments.getEventBuffer();

        OrderedEventSenderThreadQuery1 orderedEventSenderThread = new OrderedEventSenderThreadQuery1(eventBufferList);

        dataLoaderThreadPosts.start();
        dataLoaderThreadComments.start();

        orderedEventSenderThread.start();
    }

    class OrderedEventSenderThreadQuery1 extends Thread {

        private LinkedBlockingQueue<Object[]> eventBufferList[];
        public boolean doneFlag = false;

        /**
         * The constructor
         *
         * @param eventBuffer  the event buffer array
         */
        public OrderedEventSenderThreadQuery1(LinkedBlockingQueue<Object[]> eventBuffer[]) {
            super("Event Sender Query 1");
            this.eventBufferList = eventBuffer;
        }


    public void run() {
        Object[] commentEvent = null;
        Object[] postEvent = null;
        long systemCurrentTime;
        boolean firstEvent = true;
        int flag = Constants.NO_EVENT;
        boolean postLastEventArrived = false;
        boolean commentsLastEventArrived = false;
        while (true) {

            try {
                if (firstEvent) {
                    Object[] firstPostEvent = new Object[]{
                            0L,
                            -1L,
                            0L,
                            0L,
                            "",
                            "",
                            0L,
                            0L,
                            Constants.POSTS
                    };
                    systemCurrentTime = System.currentTimeMillis();
                    firstPostEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD] = systemCurrentTime;
                    ResearchEventPublisher.publishEvent(firstPostEvent, getStreamId());
                    //We print the start and the end times of the experiment even if the performance logging is disabled.
                    firstEvent = false;
                }

                try {
                    if (flag == Constants.POSTS) {
                        if(!postLastEventArrived) {
                            postEvent = eventBufferList[Constants.POSTS].take();
                            long lastEvent = (Long) postEvent[0];
                            if (lastEvent == -1L)
                            {
                                postEvent = null;
                                postLastEventArrived = true;
                            }
                        }
                    } else if (flag == Constants.COMMENTS) {
                        if(!commentsLastEventArrived) {
                            commentEvent = eventBufferList[Constants.COMMENTS].take();
                            long lastEvent = (Long) commentEvent[0];
                            if (lastEvent == -1L)
                            {
                                commentEvent = null;
                                commentsLastEventArrived = true;
                            }
                        }
                    } else {
                        if(!postLastEventArrived) {
                            postEvent = eventBufferList[Constants.POSTS].take();
                            long lastEvent = (Long) postEvent[0];
                            if (lastEvent == -1L)
                            {
                                postEvent = null;
                                postLastEventArrived = true;
                            }
                        }
                        if(!commentsLastEventArrived) {
                            commentEvent = eventBufferList[Constants.COMMENTS].take();
                            long lastEvent = (Long) commentEvent[0];
                            if (lastEvent == -1L)
                            {
                                commentEvent = null;
                                commentsLastEventArrived = true;
                            }
                        }
                    }

                } catch (Exception ex) {
                    ex.printStackTrace();
                }

                long timestampComment;
                long timestampPost;

                if (commentEvent == null) {
                    timestampComment = Long.MAX_VALUE;
                } else {
                    timestampComment = (Long) commentEvent[Constants.EVENT_TIMESTAMP_FIELD];
                }

                if (postEvent == null) {
                    timestampPost = Long.MAX_VALUE;
                } else {
                    timestampPost = (Long) postEvent[Constants.EVENT_TIMESTAMP_FIELD];
                }

                if (timestampComment < timestampPost && timestampComment != Long.MAX_VALUE) {

                    systemCurrentTime = System.currentTimeMillis();
                    commentEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD] = systemCurrentTime; //This corresponds to the iij_timestamp
                    ResearchEventPublisher.publishEvent(commentEvent, getStreamId());
                    flag = Constants.COMMENTS;
                } else if (timestampPost != Long.MAX_VALUE) {

                    systemCurrentTime = System.currentTimeMillis();
                    postEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD] = systemCurrentTime; //This corresponds to the iij_timestamp
                    ResearchEventPublisher.publishEvent(postEvent, getStreamId());
                    flag = Constants.POSTS;
                }

                //When all buffers are empty
                if (commentEvent == null && postEvent == null) {
                    //Sending second dummy event to signal end of streams
                    Object[] finalPostEvent = new Object[]{
                            0L,
                            -2L,
                            0L,
                            0L,
                            "",
                            "",
                            0L,
                            0L,
                            Constants.POSTS
                    };
                    systemCurrentTime = System.currentTimeMillis();
                    finalPostEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD] = systemCurrentTime;
                    Thread.sleep(1000);//We just sleep for short period so that we can ensure that all the data events have been processed by the ranker properly before we shutdown.
                    ResearchEventPublisher.publishEvent(finalPostEvent, getStreamId());
                    doneFlag = true;
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}
}
