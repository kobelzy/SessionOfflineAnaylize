package com.jdlzy.dao;

import com.jdlzy.domain.Task;

/**
 * 数据访问对象接口
 * <p>
 * Created by liuziyang on 2017/9/11.
 * Copyright © liuziyang ustl. All Rights Reserved
 */
public interface ITaskDAO {
    Task findById(long taskId);
}
