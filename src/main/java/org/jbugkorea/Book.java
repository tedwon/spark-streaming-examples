package org.jbugkorea;

import com.google.common.base.Objects;
import org.infinispan.commons.hash.Hash;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoMessage;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

@ProtoMessage(name = "Book")
public class Book implements Serializable {


    private String title;


    private String description;


    private int publicationYear;


    private HashSet<Author> authors;

    public Book() {
    }

    public Book(String title, String description, int publicationYear, HashSet<Author> authors) {
        this.title = title;
        this.description = description;
        this.publicationYear = publicationYear;
        this.authors = authors;
    }

    @ProtoField(number = 1, required = true)
    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @ProtoField(number = 2, required = true)
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @ProtoField(number = 3, required = true)
    public int getPublicationYear() {
        return publicationYear;
    }

    public void setPublicationYear(int publicationYear) {
        this.publicationYear = publicationYear;
    }

    @ProtoField(number = 4, required = false)
    public HashSet<Author> getAuthors() {
        return authors;
    }

    public void setAuthors(HashSet<Author> authors) {
        this.authors = authors;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Book book = (Book) o;
        return publicationYear == book.publicationYear &&
                Objects.equal(title, book.title) &&
                Objects.equal(description, book.description) &&
                Objects.equal(authors, book.authors);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(title, description, publicationYear, authors);
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Book{");
        sb.append("title='").append(title).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", publicationYear=").append(publicationYear);
        sb.append(", authors=").append(authors);
        sb.append('}');
        return sb.toString();
    }
}


