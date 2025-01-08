using FluentAssertions;
using SqlBulkCopier.FixedLength;

namespace SqlBulkCopier.Test.FixedLength;

public class ByteArrayExtensionsTests
{
    public class StartsWith
    {
        [Fact]
        public void WithNullSource_ShouldThrowArgumentNullException()
        {
            // Arrange
            byte[] source = null!;
            byte[] pattern = [1, 2];

            // Act
            Action act = () => source.StartsWith(pattern);

            // Assert
            act.Should().Throw<ArgumentNullException>()
                .And.ParamName.Should().Be("source");
        }

        [Fact]
        public void WithNullPattern_ShouldThrowArgumentNullException()
        {
            // Arrange
            byte[] source = [1, 2, 3];
            byte[] pattern = null!;

            // Act
            Action act = () => source.StartsWith(pattern);

            // Assert
            act.Should().Throw<ArgumentNullException>()
                .And.ParamName.Should().Be("pattern");
        }

        [Theory]
        [InlineData(new byte[] { 1, 2, 3 }, new byte[] { 1, 2 }, true)]
        [InlineData(new byte[] { 1, 2, 3 }, new byte[] { 2, 3 }, false)]
        [InlineData(new byte[] { 1, 2, 3 }, new byte[] { 1, 2, 3 }, true)]
        [InlineData(new byte[] { 1 }, new byte[] { 1, 2 }, false)]
        public void WithValidInputs_ShouldReturnExpectedResult(byte[] source, byte[] pattern, bool expected)
        {
            // Act
            var result = source.StartsWith(pattern);

            // Assert
            result.Should().Be(expected);
        }
    }

    public class EndsWith
    {
        [Fact]
        public void WithNullSource_ShouldThrowArgumentNullException()
        {
            // Arrange
            byte[] source = null!;
            byte[] pattern = [1, 2];

            // Act
            Action act = () => source.EndsWith(pattern);

            // Assert
            act.Should().Throw<ArgumentNullException>()
                .And.ParamName.Should().Be("source");
        }

        [Fact]
        public void WithNullPattern_ShouldThrowArgumentNullException()
        {
            // Arrange
            byte[] source = [1, 2, 3];
            byte[] pattern = null!;

            // Act
            Action act = () => source.EndsWith(pattern);

            // Assert
            act.Should().Throw<ArgumentNullException>()
                .And.ParamName.Should().Be("pattern");
        }

        [Theory]
        [InlineData(new byte[] { 1, 2, 3 }, new byte[] { 2, 3 }, true)]
        [InlineData(new byte[] { 1, 2, 3 }, new byte[] { 1, 2 }, false)]
        [InlineData(new byte[] { 1, 2, 3 }, new byte[] { 1, 2, 3 }, true)]
        [InlineData(new byte[] { 1 }, new byte[] { 1, 2 }, false)]
        public void WithValidInputs_ShouldReturnExpectedResult(byte[] source, byte[] pattern, bool expected)
        {
            // Act
            var result = source.EndsWith(pattern);

            // Assert
            result.Should().Be(expected);
        }
    }

    public class SequenceEqualsOptimized
    {
        [Fact]
        public void WithNullSource_ShouldThrowArgumentNullException()
        {
            // Arrange
            byte[] source = null!;
            byte[] other = [1, 2];

            // Act
            Action act = () => source.SequenceEqualsOptimized(other);

            // Assert
            act.Should().Throw<ArgumentNullException>()
                .And.ParamName.Should().Be("source");
        }

        [Fact]
        public void WithNullOther_ShouldThrowArgumentNullException()
        {
            // Arrange
            byte[] source = [1, 2, 3];
            byte[] other = null!;

            // Act
            Action act = () => source.SequenceEqualsOptimized(other);

            // Assert
            act.Should().Throw<ArgumentNullException>()
                .And.ParamName.Should().Be("other");
        }

        [Theory]
        [InlineData(new byte[] { 1, 2, 3 }, new byte[] { 1, 2, 3 }, true)]
        [InlineData(new byte[] { 1, 2, 3 }, new byte[] { 1, 2 }, false)]
        [InlineData(new byte[] { 1, 2, 3 }, new byte[] { 3, 2, 1 }, false)]
        [InlineData(new byte[] { }, new byte[] { }, true)]
        public void WithValidInputs_ShouldReturnExpectedResult(byte[] source, byte[] other, bool expected)
        {
            // Act
            var result = source.SequenceEqualsOptimized(other);

            // Assert
            result.Should().Be(expected);
        }

        [Fact]
        public void WithLargeArrays_ShouldWorkCorrectly()
        {
            // Arrange
            var source = new byte[10000];
            var other = new byte[10000];
            for (var i = 0; i < 10000; i++)
            {
                source[i] = (byte)(i % 256);
                other[i] = (byte)(i % 256);
            }

            // Act
            var result = source.SequenceEqualsOptimized(other);

            // Assert
            result.Should().BeTrue();
        }
    }
}