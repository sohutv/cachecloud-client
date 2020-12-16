package com.sohu.tv.builder.crossroom;

import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRule;
import com.alibaba.csp.sentinel.slots.block.degrade.circuitbreaker.CircuitBreakerStrategy;
import redis.clients.jedis.util.SentinelResources;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wenruiwu
 * @create 2020/12/1 11:05
 * @description
 */
public class CrossRoomSentinelConfig {

    private List<DegradeRule> degradeRules;

    public CrossRoomSentinelConfig(List<DegradeRule> degradeRules) {
        this.degradeRules = degradeRules;
    }

    public List<DegradeRule> getDegradeRules() {
        return degradeRules;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        /**
         * 主机房慢调用响应时间，超过此值记为慢调用，单位ms
         */
        private int majorSlowRatioRt = 1000;
        /**
         * 副机房慢调用响应时间，超过此值记为慢调用，单位ms
         */
        private int minorSlowRatioRt = 1000;
        /**
         * 主机房慢调用比例阈值，超过此值触发熔断（调用次数大于1000）
         */
        private double majorSlowRatioThreshold = 0.5D;
        /**
         * 副机房慢调用比例阈值，超过此值触发熔断（调用次数大于1000）
         */
        private double minorSlowRatioThreshold = 0.5D;
        /**
         * 主机房慢异常比例阈值，超过此值触发熔断（调用次数大于1000）
         */
        private double majorErrorRatio = 0.5D;
        /**
         * 副机房慢异常比例阈值，超过此值触发熔断（调用次数大于1000）
         */
        private double minorErrorRatio = 0.5D;
        /**
         * 主机房熔断窗口，单位s
         */
        private int majorTimeWindow = 5;
        /**
         * 副机房熔断窗口，单位s
         */
        private int minorTimeWindow = 5;

        public CrossRoomSentinelConfig build() {
            //主机房读：异常比例规则
            DegradeRule majorReadExpRule = new DegradeRule();
            majorReadExpRule.setResource(SentinelResources.MAJOR_READ_COMMAND_KEY);
            majorReadExpRule.setGrade(CircuitBreakerStrategy.ERROR_RATIO.getType());
            majorReadExpRule.setCount(majorErrorRatio);
            majorReadExpRule.setTimeWindow(majorTimeWindow);
            //主机房读：慢调用规则
            DegradeRule majorReadSlowRatioRule = new DegradeRule();
            majorReadSlowRatioRule.setResource(SentinelResources.MAJOR_READ_COMMAND_KEY);
            majorReadSlowRatioRule.setGrade(CircuitBreakerStrategy.SLOW_REQUEST_RATIO.getType());
            majorReadSlowRatioRule.setCount(majorSlowRatioRt);
            majorReadSlowRatioRule.setSlowRatioThreshold(majorSlowRatioThreshold);
            majorReadSlowRatioRule.setTimeWindow(majorTimeWindow);
            //主机房写：异常比例规则
            DegradeRule majorWriteExpRule = new DegradeRule();
            majorWriteExpRule.setResource(SentinelResources.MAJOR_WRITE_COMMAND_KEY);
            majorWriteExpRule.setGrade(CircuitBreakerStrategy.ERROR_RATIO.getType());
            majorWriteExpRule.setCount(majorErrorRatio);
            majorWriteExpRule.setTimeWindow(majorTimeWindow);
            //主机房写：慢调用规则
            DegradeRule majorWriteSlowRatioRule = new DegradeRule();
            majorWriteSlowRatioRule.setResource(SentinelResources.MAJOR_WRITE_COMMAND_KEY);
            majorWriteSlowRatioRule.setGrade(CircuitBreakerStrategy.SLOW_REQUEST_RATIO.getType());
            majorWriteSlowRatioRule.setCount(majorSlowRatioRt);
            majorWriteSlowRatioRule.setSlowRatioThreshold(majorSlowRatioThreshold);
            majorWriteSlowRatioRule.setTimeWindow(majorTimeWindow);
            //副机房读：异常比例规则
            DegradeRule minorReadExpRule = new DegradeRule();
            minorReadExpRule.setResource(SentinelResources.MINOR_READ_COMMAND_KEY);
            minorReadExpRule.setGrade(CircuitBreakerStrategy.ERROR_RATIO.getType());
            minorReadExpRule.setCount(minorErrorRatio);
            minorReadExpRule.setTimeWindow(minorTimeWindow);
            //副机房读：慢调用规则
            DegradeRule minorReadSlowRatioRule = new DegradeRule();
            minorReadSlowRatioRule.setResource(SentinelResources.MINOR_READ_COMMAND_KEY);
            minorReadSlowRatioRule.setGrade(CircuitBreakerStrategy.SLOW_REQUEST_RATIO.getType());
            minorReadSlowRatioRule.setCount(minorSlowRatioRt);
            minorReadSlowRatioRule.setSlowRatioThreshold(minorSlowRatioThreshold);
            minorReadSlowRatioRule.setTimeWindow(minorTimeWindow);
            //副机房写：异常比例规则
            DegradeRule minorWriteExpRule = new DegradeRule();
            minorWriteExpRule.setResource(SentinelResources.MINOR_WRITE_COMMAND_KEY);
            minorWriteExpRule.setGrade(CircuitBreakerStrategy.ERROR_RATIO.getType());
            minorWriteExpRule.setCount(minorErrorRatio);
            minorWriteExpRule.setTimeWindow(minorTimeWindow);
            //副机房写：慢调用规则
            DegradeRule minorWriteSlowRatioRule = new DegradeRule();
            minorWriteSlowRatioRule.setResource(SentinelResources.MINOR_WRITE_COMMAND_KEY);
            minorWriteSlowRatioRule.setGrade(CircuitBreakerStrategy.SLOW_REQUEST_RATIO.getType());
            minorWriteSlowRatioRule.setCount(minorSlowRatioRt);
            minorWriteSlowRatioRule.setSlowRatioThreshold(minorSlowRatioThreshold);
            minorWriteSlowRatioRule.setTimeWindow(minorTimeWindow);

            List<DegradeRule> rules = new ArrayList<>();
            rules.add(majorReadExpRule);
            rules.add(majorReadSlowRatioRule);
            rules.add(majorWriteExpRule);
            rules.add(majorWriteSlowRatioRule);
            rules.add(minorReadExpRule);
            rules.add(minorReadSlowRatioRule);
            rules.add(minorWriteExpRule);
            rules.add(minorWriteSlowRatioRule);

            return new CrossRoomSentinelConfig(rules);
        }

        public Builder setMajorSlowRatioRt(int majorSlowRatioRt) {
            this.majorSlowRatioRt = majorSlowRatioRt;
            return this;
        }

        public Builder setMinorSlowRatioRt(int minorSlowRatioRt) {
            this.minorSlowRatioRt = minorSlowRatioRt;
            return this;
        }

        public Builder setMajorSlowRatioThreshold(double majorSlowRatioThreshold) {
            this.majorSlowRatioThreshold = majorSlowRatioThreshold;
            return this;
        }

        public Builder setMinorSlowRatioThreshold(double minorSlowRatioThreshold) {
            this.minorSlowRatioThreshold = minorSlowRatioThreshold;
            return this;
        }

        public Builder setMajorErrorRatio(double majorErrorRatio) {
            this.majorErrorRatio = majorErrorRatio;
            return this;
        }

        public Builder setMinorErrorRatio(double minorErrorRatio) {
            this.minorErrorRatio = minorErrorRatio;
            return this;
        }

        public Builder setMajorTimeWindow(int majorTimeWindow) {
            this.majorTimeWindow = majorTimeWindow;
            return this;
        }

        public Builder setMinorTimeWindow(int minorTimeWindow) {
            this.minorTimeWindow = minorTimeWindow;
            return this;
        }

    }
}
