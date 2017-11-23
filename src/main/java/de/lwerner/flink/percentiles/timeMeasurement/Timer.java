package de.lwerner.flink.percentiles.timeMeasurement;

import java.util.HashMap;

/**
 * Class Timer
 *
 * Handles multiple timer by string identifiers.
 *
 * @author Lukas Werner
 */
public class Timer {

    /**
     * Default timer name
     */
    private static final String DEFAULT_TIMER_NAME = "default";

    /**
     * The actual timers (start times)
     */
    private HashMap<String, Long> timers;
    /**
     * The timer results (running times)
     */
    private HashMap<String, Long> timerResults;

    /**
     * Initializes collections
     */
    public Timer() {
        timers = new HashMap<>();
        timerResults = new HashMap<>();
    }

    /**
     * Get all the timer results
     *
     * @return all timer results
     */
    public HashMap<String, Long> getTimerResults() {
        return timerResults;
    }

    /**
     * Start the default timer
     *
     * @throws Exception if timer already runs
     */
    public void startTimer() throws Exception {
        startTimer(DEFAULT_TIMER_NAME);
    }

    /**
     * Start custom timer by name
     *
     * @param timerName timer name
     *
     * @throws Exception if timer already runs
     */
    public void startTimer(String timerName) throws Exception {
        Long time = timers.get(timerName);

        if (time != null) {
            throw new Exception(String.format("Timer %s already runs!", timerName));
        }

        timers.put(timerName, System.currentTimeMillis());
    }

    /**
     * Stop default timer
     *
     * @throws Exception if timer doesn't run yet
     */
    public void stopTimer() throws Exception {
        stopTimer(DEFAULT_TIMER_NAME);
    }

    /**
     * Stop custom timer by name
     *
     * @param timerName timer name
     *
     * @throws Exception if timer doesn't run yet
     */
    public void stopTimer(String timerName) throws Exception {
        long now = System.currentTimeMillis();

        Long startTime = timers.get(timerName);

        if (startTime == null) {
            throw new Exception(String.format("Timer %s isn't running yet!", timerName));
        }

        timers.remove(timerName);

        long runningTime = now - startTime;

        timerResults.put(timerName, runningTime);
    }

    /**
     * Get the result for the default timer
     *
     * @return the running time in ms
     *
     * @throws Exception if there is no result for the default timer present
     */
    public long getTimerResult() throws Exception {
        return getTimerResult(DEFAULT_TIMER_NAME);
    }

    /**
     * Get the result for a custom timer by name
     *
     * @param timerName timer name
     *
     * @return the running time in ms
     *
     * @throws Exception if there is no result for this timer present
     */
    public long getTimerResult(String timerName) throws Exception {
        Long timerResult = timerResults.get(timerName);

        if (timerResult == null) {
            throw new Exception(String.format("No timer result for timer %s!", timerName));
        }

        return timerResult;
    }

}