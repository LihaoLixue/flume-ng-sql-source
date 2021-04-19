package org.keedio.flume.metrics;

import javax.persistence.Entity;

@Entity
public class trade {
    private String khh;
    private String kyzc;

    public String getKhh() {
        return khh;
    }

    public void setKhh(String khh) {
        this.khh = khh;
    }

    public String getKyzc() {
        return kyzc;
    }

    public void setKyzc(String kyzc) {
        this.kyzc = kyzc;
    }
}
