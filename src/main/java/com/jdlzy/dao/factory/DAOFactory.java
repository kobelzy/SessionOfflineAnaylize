package com.jdlzy.dao.factory;


import com.jdlzy.dao.ITaskDAO;
import com.jdlzy.dao.impl.TaskDAOImpl;

/**
 * 数据访问对象工厂类
 * <p>
 * Created by liuziyang on 2017/9/11.
 * Copyright © liuziyang ustl. All Rights Reserved
 */
public class DAOFactory {
    /**
     * 构造并返回TaskDAO实例
     *
     * @return
     */
    public static ITaskDAO getTaskDAO() {
        return new TaskDAOImpl();
    }
}
