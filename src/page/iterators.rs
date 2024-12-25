use std::io::Read;

use rkyv::{api::high::HighDeserializer, Archive};

use crate::{page::util::parse_general_header, persistence::data::{rkyv_data::parse_archived_row, DataTypeValue}, IndexData, Link};

use super::{index::IndexValue, seek_by_link, seek_to_page_start, Interval};

pub struct PageIterator {
    intervals: Vec<Interval>,
    current_intervals_index: usize,
    current_position_in_interval: usize,
}

impl PageIterator {
    pub fn new(intervals: Vec<Interval>) -> PageIterator {
        PageIterator {
            current_intervals_index: 0,
            current_position_in_interval: if intervals.len() > 0 {
                intervals[0].0
            } else {
                0
            },
            intervals,
        }
    }
}

impl Iterator for PageIterator {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        let mut result: Option<Self::Item> = None;

        if self.current_intervals_index >= self.intervals.len() {
            result = None
        } else if self.current_position_in_interval
            >= self.intervals[self.current_intervals_index].0
            && self.current_position_in_interval <= self.intervals[self.current_intervals_index].1
        {
            result = Some(self.current_position_in_interval as u32);
            self.current_position_in_interval += 1;
        } else if self.current_position_in_interval > self.intervals[self.current_intervals_index].1
        {
            self.current_intervals_index += 1;
            if self.current_intervals_index >= self.intervals.len() {
                result = None;
            } else {
                self.current_position_in_interval = self.intervals[self.current_intervals_index].0;
                result = Some(self.current_position_in_interval as u32);
                self.current_position_in_interval += 1;
            }
        }

        result
    }
}

struct LinksIterator<'a, T>
where
    T: Archive,
    <T as rkyv::Archive>::Archived: rkyv::Deserialize<T, HighDeserializer<rkyv::rancor::Error>>,
{
    file: &'a mut std::fs::File,
    page_id: u32,
    index_records: Option<Vec<IndexValue<T>>>,
    index_record_index: usize,
}

impl<T> LinksIterator<'_, T>
where
    T: Archive,
    <T as rkyv::Archive>::Archived: rkyv::Deserialize<T, HighDeserializer<rkyv::rancor::Error>>,
{
    pub fn new(file: &mut std::fs::File, page_id: u32) -> LinksIterator<'_, T> {
        LinksIterator {
            file,
            page_id,
            index_records: None,
            index_record_index: 0,
        }
    }
}

impl<T> Iterator for LinksIterator<'_, T>
where
    T: Archive,
    <T as rkyv::Archive>::Archived: rkyv::Deserialize<T, HighDeserializer<rkyv::rancor::Error>>,
{
    type Item = Link;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index_records.is_none() {
            seek_to_page_start(&mut self.file, self.page_id).expect("page should be seekable");
            let header = parse_general_header(&mut self.file).expect("header should be readable");

            let mut buffer: Vec<u8> = vec![0u8; header.data_length as usize];
            self.file
                .read_exact(&mut buffer)
                .expect("index data should be readable");
            let archived = unsafe {
                rkyv::access_unchecked::<<IndexData<T> as Archive>::Archived>(&buffer[..])
            };
            self.index_records = Some(
                rkyv::deserialize::<IndexData<T>, _>(archived)
                    .expect("data should be valid")
                    .index_values,
            );
        }

        if self.index_record_index < self.index_records.as_deref().unwrap().len() {
            let result = Some(
                self.index_records.as_deref().unwrap()[self.index_record_index]
                    .link
                    .clone(),
            );
            self.index_record_index += 1;
            result
        } else {
            None
        }
    }
}

struct DataIterator<'a> {
    file: &'a mut std::fs::File,
    schema: Vec<(String, String)>,
    links: Vec<Link>,
    link_index: usize,
}

impl DataIterator<'_> {
    pub fn new(file: &mut std::fs::File, schema: Vec<(String, String)>, mut links: Vec<Link>) -> DataIterator<'_> {
        links.sort_by(|a, b| (a.page_id, a.offset).partial_cmp(&(b.page_id, b.offset)).unwrap());

        DataIterator {
            file,
            schema,
            links,
            link_index: 0,
        }
    }
}

impl Iterator for DataIterator<'_> {
    type Item = Vec<DataTypeValue>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.link_index >= self.links.len() {
            return None;
        }

        let current_link = self.links[self.link_index];
        seek_by_link(&mut self.file, current_link).expect("the seek should be successful");
        let mut buffer = vec![0u8; current_link.length as usize];
        self.file.read_exact(&mut buffer).expect("the data should be read");
        let row = parse_archived_row(&buffer, &self.schema);

        self.link_index += 1;

        Some(row)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        page::{self, iterators::DataIterator, util::parse_space_info, PageId}, persistence::data::DataTypeValue, Interval, Link, PAGE_SIZE
    };

    use super::{LinksIterator, PageIterator};

    #[test]
    fn test_page_iterator() {
        let interval1 = Interval(1, 2);
        let interval2 = Interval(5, 7);
        let page_iterator = PageIterator::new(vec![interval1, interval2]);
        let collected = page_iterator.collect::<Vec<_>>();
        assert_eq!(collected, vec![1, 2, 5, 6, 7]);
    }

    #[test]
    fn test_page_iterator_empty() {
        let page_iterator = PageIterator::new(vec![]);
        let collected = page_iterator.collect::<Vec<_>>();
        assert_eq!(collected, Vec::<u32>::new());
    }

    #[test]
    fn test_links_iterator() {
        let filename = "tests/data/table_links_test.wt";
        super::super::util::test::create_test_database_file(filename);

        let mut file = std::fs::File::open(filename).unwrap();
        let links = LinksIterator::<'_, i32>::new(&mut file, 1);
        assert_eq!(
            links.collect::<Vec<_>>(),
            vec![
                Link {
                    page_id: PageId(2),
                    offset: 0,
                    length: 24
                },
                Link {
                    page_id: PageId(2),
                    offset: 24,
                    length: 28
                }
            ]
        );
    }

    #[test]
    fn test_pages_and_links_iterators() {
        let filename = "tests/data/table_pages_and_links_test.wt";
        super::super::util::test::create_test_database_file(filename);

        let mut file = std::fs::File::open(filename).unwrap();
        let space_info = parse_space_info::<PAGE_SIZE>(&mut file).unwrap();
        let index_intervals = space_info.primary_key_intervals;

        let pages_ids = PageIterator::new(index_intervals).collect::<Vec<_>>();
        assert_eq!(pages_ids, vec![1]);

        let links = LinksIterator::<'_, i32>::new(&mut file, pages_ids[0]).collect::<Vec<_>>();
        let data_iterator: DataIterator<'_> = DataIterator::new(&mut file, space_info.row_schema, links);
        assert_eq!(data_iterator.collect::<Vec<_>>(), vec![
            vec![DataTypeValue::I32(1), DataTypeValue::String("first string".to_string())],
            vec![DataTypeValue::I32(2), DataTypeValue::String("second string".to_string())]
        ]);
    }
}
