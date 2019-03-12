package kv.key;

import kv.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class ContactDimension extends BaseDimension {
    private String telephone;
    private String name;

    public ContactDimension(){
        super();
    }

    public ContactDimension(String telephone, String name) {
        super();
        this.telephone = telephone;
        this.name = name;
    }



    public String getTelephone() {
        return telephone;
    }

    public void setTelephone(String telephone) {
        this.telephone = telephone;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ContactDimension that = (ContactDimension) o;
        return Objects.equals(telephone, that.telephone) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(telephone, name);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.telephone = in.readUTF();
        this.name = in.readUTF();
    }

    @Override
    public int compareTo(Object o) {

        ContactDimension anotherContactDimension = (ContactDimension) o;

        int result = this.name.compareTo(anotherContactDimension.name);
        if(result != 0) return result;

        result = this.telephone.compareTo(anotherContactDimension.telephone);
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(this.telephone);
        out.writeUTF(this.name);

    }
}
