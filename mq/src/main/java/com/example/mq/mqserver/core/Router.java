package com.example.mq.mqserver.core;


/*
*  交换机的转发规则
*  验证 bindingKey 是否合法
*/

import com.example.mq.common.MqException;

public class Router {

    // bindingKey 构造规则:
    // 1.数字、字母、下划线
    // 2.使用 . 分割成若干部分
    // 3.允许存在 * 和 # 作为通配符，但是通配符只能作为独立的分段
    public boolean checkBindingKey(String bindingKey) {
        if(bindingKey.length() == 0) {
            return true;
        }
        // 字符是否合法
        for (int i = 0; i < bindingKey.length(); i++) {
            char ch = bindingKey.charAt(i);
            if(ch >= 'a' && ch <= 'z') {
                continue;
            }
            if(ch >= 'A' && ch <= 'Z') {
                continue;
            }
            if(ch >= '0' && ch <= '9') {
                continue;
            }
            if(ch == '_' || ch == '.' || ch == '*' || ch == '#') {
                continue;
            }
            return false;
        }

        // 检查 * # 是否时独立部分
        String[] words = bindingKey.split("\\.");
        for(String word : words) {
            if(word.length() > 1 && (word.contains("*") || word.contains("#"))) {
                return false;
            }
        }

        // 约定通配符间的相邻关系
        for (int i = 0; i < words.length - 1; i++) {
            if(words[i].equals("#") && words[i+1].equals("#")) { // 连续两个##
                return false;
            }
            if(words[i].equals("#") && words[i+1].equals("*")) { // # 连着 *
                return false;
            }
            if(words[i].equals("*") && words[i+1].equals("#")) { // * 连着 #
                return false;
            }
        }
        return true;
    }

    // routingKey 构造规则：
    // 1.数字、字母、下划线
    // 2.使用 . 分割成若干部分
    public boolean checkRoutingKey(String routingKey) {
        if(routingKey.length() == 0) { // 空字符串
            return true;
        }
        // 字符是否合法
        for (int i = 0; i < routingKey.length(); i++) {
            char ch = routingKey.charAt(i);
            if(ch >= 'a' && ch <= 'z') {
                continue;
            }
            if(ch >= 'A' && ch <= 'Z') {
                continue;
            }
            if(ch >= '0' && ch <= '9') {
                continue;
            }
            if(ch == '_' || ch == '.') {
                continue;
            }
            return false;
        }
        return true;
    }



    public boolean route(ExchangeType exchangeType, Binding binding, Message message) throws MqException {
        if(exchangeType == ExchangeType.FANOUT) {
            return true;
        } else if(exchangeType == ExchangeType.TOPIC) {
            return routeTopic(binding, message);
        } else {
            throw new MqException("[Router] 交换机类型非法！ exchangeType=" + exchangeType);
        }
    }

    private boolean routeTopic(Binding binding, Message message) {
        String[] bindingTokens = binding.getBindingKey().split("\\.");
        String[] routingTokens = message.getRoutingKey().split("\\.");

        int bindingIndex = 0; int routingIndex = 0;
        while (bindingIndex < bindingTokens.length && routingIndex < routingTokens.length) {
            if(bindingTokens[bindingIndex].equals("*")) {
                bindingIndex++;
                routingIndex++;
                continue;
            } else if(bindingTokens[bindingIndex].equals("#")) {
                bindingIndex++;
                if(bindingIndex == bindingTokens.length) {
                    return true;
                }
                routingIndex = findNextMatch(routingTokens, routingIndex, bindingTokens[bindingIndex]);
                if(routingIndex == -1) {
                    return false;
                }
                bindingIndex++;
                routingIndex++;
            } else { // 普通字符串
                if(!bindingTokens[bindingIndex].equals(routingTokens[routingIndex])) {
                    return false;
                }
                bindingIndex++;
                routingIndex++;
            }
        }
        if(bindingIndex == bindingTokens.length && routingIndex == bindingTokens.length)  {
            return true;
        }
        return false;
    }

    private int findNextMatch(String[] routingTokens, int routingIndex, String bindingToken) {
        for (int i = routingIndex; i < routingTokens.length; i++) {
            if(routingTokens[i].equals(bindingToken)) {
                return i;
            }
        }
        return -1;
    }
}
