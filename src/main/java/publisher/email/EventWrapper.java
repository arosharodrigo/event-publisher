package publisher.email;

public class EventWrapper {

    private Object[] event;
    private int eventSizeInBytes;

    public EventWrapper(Object[] event, int eventSizeInBytes) {
        this.event = event;
        this.eventSizeInBytes = eventSizeInBytes;
    }

    public Object[] getEvent() {
        return event;
    }

    public int getEventSizeInBytes() {
        return eventSizeInBytes;
    }
}
