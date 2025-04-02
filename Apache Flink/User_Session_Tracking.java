// User event data model
public class UserEvent {
    public String userId;
    public String eventType;
    public long timestamp;
    
    public UserEvent() {}
    
    public UserEvent(String userId, String eventType, long timestamp) {
        this.userId = userId;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }
}

// User session tracking implementation
public class UserSessionTracker {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        DataStream<UserEvent> userEvents = env
            .addSource(new FlinkKafkaConsumer<>("user-events", new JSONKeyValueDeserializationSchema(), properties))
            .map(value -> {
                JSONObject json = (JSONObject) value.get("value");
                return new UserEvent(
                    json.getString("userId"),
                    json.getString("eventType"),
                    json.getLong("timestamp")
                );
            });
        
        DataStream<String> sessionAlerts = userEvents
            .keyBy(event -> event.userId)
            .process(new SessionTracker(30 * 60 * 1000)); // 30-minute inactivity
        
        sessionAlerts.print();
        
        env.execute("User Session Tracking");
    }
    
    public static class SessionTracker 
        extends KeyedProcessFunction<String, UserEvent, String> {
        
        private final long sessionTimeout;
        private ValueState<Long> lastActivityState;
        private ValueState<Integer> eventCountState;
        
        public SessionTracker(long sessionTimeout) {
            this.sessionTimeout = sessionTimeout;
        }
        
        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Long> lastActivityDesc = 
                new ValueStateDescriptor<>("lastActivity", Long.class);
            lastActivityState = getRuntimeContext().getState(lastActivityDesc);
            
            ValueStateDescriptor<Integer> eventCountDesc = 
                new ValueStateDescriptor<>("eventCount", Integer.class);
            eventCountState = getRuntimeContext().getState(eventCountDesc);
        }
        
        @Override
        public void processElement(
            UserEvent event,
            Context ctx,
            Collector<String> out) throws Exception {
            
            Long lastActivity = lastActivityState.value();
            Integer count = eventCountState.value();
            
            if (count == null) {
                count = 0;
            }
            
            // If first event or session expired
            if (lastActivity == null || 
                (event.timestamp - lastActivity) > sessionTimeout) {
                
                if (lastActivity != null) {
                    // Report previous session
                    out.collect("Session ended for user " + event.userId + 
                              " with " + count + " events");
                }
                
                // Start new session
                count = 0;
                out.collect("New session started for user " + event.userId);
            }
            
            // Update state
            count++;
            eventCountState.update(count);
            lastActivityState.update(event.timestamp);
            
            // Schedule timer for expiration
            ctx.timerService().registerEventTimeTimer(event.timestamp + sessionTimeout);
        }
        
        @Override
        public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<String> out) throws Exception {
            
            // If no new events arrived by this time
            Long lastActivity = lastActivityState.value();
            if (lastActivity != null && 
                (timestamp - lastActivity) >= sessionTimeout) {
                
                Integer count = eventCountState.value();
                out.collect("Session timeout for user " + ctx.getCurrentKey() + 
                          " with " + count + " events");
                
                // Clear state
                lastActivityState.clear();
                eventCountState.clear();
            }
        }
    }
}