package de.mindconsulting.s3storeboot.jaxb.meta;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Map;

@XmlRootElement(name="Meta")
public class StorageMeta {

    private Map<String,String> meta;

    public StorageMeta(){}

    public StorageMeta(Map<String,String> meta) {
        this.meta = meta;
    }

    @XmlElement(name="Data")
    @XmlElementWrapper(name="Entries")
    public Map<String, String> getMeta() {
        return meta;
    }

    public void setMeta(Map<String,String> meta) {
        this.meta = meta;
    }
}
