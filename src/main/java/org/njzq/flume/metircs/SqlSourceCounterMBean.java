package org.njzq.flume.metircs;

/**
 * @author LH
 * @description: bean接口
 * @date 2021-04-19 13:56
 */
public interface SqlSourceCounterMBean {
    public long getEventCount();
    public void incrementEventCount(int value);
    public long getAverageThroughput();
    public long getCurrentThroughput();
    public long getMaxThroughput();
}
