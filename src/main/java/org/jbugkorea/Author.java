package org.jbugkorea;

import com.google.common.base.Objects;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoMessage;

import java.io.Serializable;

@ProtoMessage(name = "Author")
public class Author implements Serializable {

    private String name;
    private String surname;

    public Author() {
    }

    public Author(String name, String surname) {
        this.name = name;
        this.surname = surname;
    }

    @ProtoField(number = 1, required = true)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @ProtoField(number = 2, required = false)
    public String getSurname() {
        return surname;
    }

    public void setSurname(String surname) {
        this.surname = surname;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Author author = (Author) o;
        return Objects.equal(name, author.name) &&
                Objects.equal(surname, author.surname);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, surname);
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Author{");
        sb.append("name='").append(name).append('\'');
        sb.append(", surname='").append(surname).append('\'');
        sb.append('}');
        return sb.toString();
    }
}

