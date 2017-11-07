package streaming;

import java.io.Serializable;

/**
 * Created by jiangtao7 on 2017/11/7.
 * {"stay_time":0,"appId":"com.gome.ecloud","osVer":"7.1.1","pageId":"CXFCombinedActivity",
 * "deviceModel":"vivo X9","lang":"zh","city":"","moduleVer":"1.1.0","timestamp":"2017-10-25T02:03:28.707Z","osname":"Android","nettype":"4G",
 * "province":"江苏","action":"Leave","screen":"1080*1920","moduleId":"113","imei":"864552031319317","country":"中国","ip":"117.136.66.184",
 * "deviceBrand":"vivo"}

 */
public class DeviceLog implements Serializable{
    private long stay_time;
    private String appId;
    private String osVer;
    private String pageId;
    private String deviceModel;
    private String lang;
    private String city;
    private String timestamp;
    private String osname;
    private String nettype;
    private String province;
    private String action;
    private String screen;
    private String moduleId;
    private String imei;
    private String country;
    private String ip;
    private String deviceBrand;

    public long getStay_time() {
        return stay_time;
    }

    public void setStay_time(long stay_time) {
        this.stay_time = stay_time;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getOsVer() {
        return osVer;
    }

    public void setOsVer(String osVer) {
        this.osVer = osVer;
    }

    public String getPageId() {
        return pageId;
    }

    public void setPageId(String pageId) {
        this.pageId = pageId;
    }

    public String getDeviceModel() {
        return deviceModel;
    }

    public void setDeviceModel(String deviceModel) {
        this.deviceModel = deviceModel;
    }

    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getOsname() {
        return osname;
    }

    public void setOsname(String osname) {
        this.osname = osname;
    }

    public String getNettype() {
        return nettype;
    }

    public void setNettype(String nettype) {
        this.nettype = nettype;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getScreen() {
        return screen;
    }

    public void setScreen(String screen) {
        this.screen = screen;
    }

    public String getModuleId() {
        return moduleId;
    }

    public void setModuleId(String moduleId) {
        this.moduleId = moduleId;
    }

    public String getImei() {
        return imei;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getDeviceBrand() {
        return deviceBrand;
    }

    public void setDeviceBrand(String deviceBrand) {
        this.deviceBrand = deviceBrand;
    }

    @Override
    public String toString() {
        return "DeviceLog{" +
                "stay_time=" + stay_time +
                ", appId='" + appId + '\'' +
                ", osVer='" + osVer + '\'' +
                ", pageId='" + pageId + '\'' +
                ", deviceModel='" + deviceModel + '\'' +
                ", lang='" + lang + '\'' +
                ", city='" + city + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", osname='" + osname + '\'' +
                ", nettype='" + nettype + '\'' +
                ", province='" + province + '\'' +
                ", action='" + action + '\'' +
                ", screen='" + screen + '\'' +
                ", moduleId='" + moduleId + '\'' +
                ", imei='" + imei + '\'' +
                ", country='" + country + '\'' +
                ", ip='" + ip + '\'' +
                ", deviceBrand='" + deviceBrand + '\'' +
                '}';
    }
}
