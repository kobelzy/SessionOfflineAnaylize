package com.jdlzy.dao.impl;

/**
 * 配置加载管理类
 * <p>
 * Created by liuziyang on 2017/9/11.
 * Copyright © liuziyang ustl. All Rights Reserved
 */


import com.jdlzy.dao.ITaskDAO;
import com.jdlzy.domain.Task;
import com.jdlzy.jdbc.JDBCHelper;

import java.sql.ResultSet;

public class TaskDAOImpl implements ITaskDAO {
    /**
     * 构造并返回TaskDAO实例
     *
     * @param taskId
     * @return
     */
    public Task findById(long taskId) {
        final Task task = new Task();
        String sql = "SELECT * FROM task WHERE task_id= ?";
        Object[] params = {taskId};

        JDBCHelper jdbcHelper = JDBCHelper.getInstanse();
        jdbcHelper.executeQuery(sql, params, new JDBCHelper.QueryCallback() {
            public void process(ResultSet rs) throws Exception {
                if (rs.next()) {
                    long taskId = rs.getLong(1);
                    String taskName = rs.getString(2);
                    String createTime = rs.getString(3);
                    String startTime = rs.getString(4);
                    String finishTime =rs.getString(5);
                    String taskType =rs.getString(6);
                    String taskStatus = rs.getString(7);
                    String taskParam = rs.getString(8);
                    task.setTaskId(taskId);
                    task.setTaskName(taskName);
                    task.setCreateTime(createTime);
                    task.setStartTime(startTime);
                    task.setFinishTime(finishTime);
                    task.setTaskType(taskType);
                    task.setTaskStatus(taskStatus);
                    task.setTaskParam(taskParam);
                }
            }
        });

        return task;
    }
}