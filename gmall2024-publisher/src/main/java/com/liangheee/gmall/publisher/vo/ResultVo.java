package com.liangheee.gmall.publisher.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author liangheee
 * * @date 2024-12-10
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class ResultVo {
    private Integer status;
    private String msg;
    private Object data;

    public ResultVo status(Integer status){
        this.status = status;
        return this;
    }

    public ResultVo msg(String msg){
        this.msg = msg;
        return this;
    }

    public ResultVo data(Object data){
        this.data = data;
        return this;
    }

    public static ResultVo ok(){
        ResultVo resultVo = new ResultVo();
        resultVo.status = 0;
        return resultVo;
    }

    public static ResultVo ok(String msg,Object data){
        return new ResultVo(0,msg,data);
    }

    public static ResultVo ok(Integer status,String msg,Object data){
        return new ResultVo(status,msg,data);
    }

    public static ResultVo error(){
        ResultVo resultVo = new ResultVo();
        resultVo.setStatus(1);
        return resultVo;
    }

    public static ResultVo error(String msg,Object data){
        return new ResultVo(1,msg,data);
    }

    public static ResultVo error(Integer status,String msg,Object data){
        return new ResultVo(status,msg,data);
    }
}
