package com.jdlzy.exception;

/**
 * 任务异常类
 * <p>
 * Created by liuziyang on 2017/9/11.
 * Copyright © liuziyang ustl. All Rights Reserved
 */
public class TaskException extends Exception {
    /**
     * Constructs an TaskException with nothing.
     */
    public TaskException() {
        super();
    }

    /**
     * Constructs an TaskException with the specified detail message.
     *
     * @param message
     */
    public TaskException(String message) {
        super(message);
    }

    /**
     * Constructs an TaskException with the specified detail message and cause.
     *
     * @param message
     * @param cause
     */
    public TaskException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs an TaskException with the specified cause
     *
     * @param cause
     */
    public TaskException(Throwable cause) {
        super(cause);
    }

}
