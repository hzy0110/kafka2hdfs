package com.hzy.test;

import com.hzy.util.DateUtil;

/**
 * Created by Hzy on 2016/8/4.
 */
public class test {
    public static void main(String[] args){
//        String str = "cd:20160803110003,aid:zhongxin,c:201607121101,adt:0,i:543,ap:513,dev:VLAN_20150609f655bebd-8d04-427a-bd03-ab697c96115a,proid:3305,m:185936162D75,ip:192.168.34.53,in:1609,t:PIC,ct:CE,os:ios9,bro:safari";
//        String date = str.substring(5,15);
//        String year = date.substring(0,2);
//        String month = date.substring(2,4);
//        String day = date.substring(4,6);
//        String hour = date.substring(6,8);
//        String minute = date.substring(8,10);
//        System.out.println(date);
//        System.out.println(year);
//        System.out.println(month);
//        System.out.println(day);
//        System.out.println(hour);
//        System.out.println(minute);




//        String date = DateUtil.getDate();
//        String year  = date.substring(2, 4);
//        String month = date.substring(5, 7);
//        String day =date.substring(8, 10);
//        String hour = date.substring(11, 13);
//
//        String path = "/tmp/kafka/advert/"+year+"/"+month+"/"+day+"/"+hour+"/";
//        System.out.println(path);



//       String adid = "234";
//            int maxsize = 7;
//            //String adid = Integer.toString(i);
//            int idlan = adid.length();
//            int addlan =  maxsize-idlan;
//            for(int i =0;i<addlan;i++){
//                adid ="0"+ adid;
//            }
//        System.out.println(adid);

        System.out.println(DateUtil.getDateBySimpleDateFormat("yyMMddHH"));
    }
}
