package com.ggstar.util.ip;

/**
 * Created by Wang Zhe on 2015/8/11.
 */
public class IpRelation {

    private String ipStart;

    private String ipEnd;

    private int ipCode;

    private String province;

    private String city;

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public int getIpCode() {
        return ipCode;
    }

    public void setIpCode(int ipCode) {
        this.ipCode = ipCode;
    }

    public String getIpEnd() {
        return ipEnd;
    }

    public void setIpEnd(String ipEnd) {
        this.ipEnd = ipEnd;
    }

    public String getIpStart() {
        return ipStart;
    }

    public void setIpStart(String ipStart) {
        this.ipStart = ipStart;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }
}
